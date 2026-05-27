"""respx regression-pins for hippius_hub.console.

console.py talks to api.hippius.com through a single `_request(method, path, ...)`
helper. Each of its 28 public functions builds a path, picks a verb, and
sometimes ships a JSON body. A typo in any of those (renamed JSON field,
wrong HTTP verb, wrong URL path) ships silently today — these tests give us
one regression-pin per function so a refactor that breaks the request shape
fails a test instead of a user.

These are NOT branch-coverage tests; the goal is "request shape correct AND
response decode correct" on the happy path, plus one ConsoleError variant
per function family so the typed-error contract is also pinned.

The respx mock URL is `https://api.test.invalid` (RFC 6761 reserved, never
resolves) so any test that forgets to patch the URL constant blows up on
DNS rather than silently leaking to production.

The dual `monkeypatch.setattr` of `constants.DEFAULT_API_URL` AND
`console.DEFAULT_API_URL` mirrors the registry-side dual-patch pattern in
`tests/respx_fixtures.py::monkeypatched_registry`: `console.py` did
`from .constants import DEFAULT_API_URL` at import time which copies the
value into the `hippius_hub.console` namespace; patching only the source
leaves the consumer module pointed at the production URL.
"""
from __future__ import annotations

import httpx
import pytest
import respx

from hippius_hub import console
from hippius_hub.console import ConsoleError


MOCK_API = "https://api.test.invalid"


@pytest.fixture
def monkeypatched_console_api(monkeypatch):
    """Point both `DEFAULT_API_URL` bindings at the mock URL.

    Dual-patch rationale: see module docstring. Returns MOCK_API so tests
    can build URLs without re-importing the constant.
    """
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_API_URL", MOCK_API)
    monkeypatch.setattr("hippius_hub.console.DEFAULT_API_URL", MOCK_API)
    return MOCK_API


@pytest.fixture
def with_token(monkeypatch):
    """Make `_headers()` find a saved token without touching the filesystem.

    `load_api_token` reads from `API_TOKEN_PATH`; rather than write a real
    file and chmod it, we stub it directly. Tests that exercise the
    "missing token" branch can pass a literal `token=` to the function
    under test or omit this fixture.
    """
    monkeypatch.setattr(console, "load_api_token", lambda: "test-token")
    return "test-token"


# -------- _request authentication / error decoding (shared helper) --------

@respx.mock
def test_request_sends_token_header(monkeypatched_console_api, with_token):
    """Every authenticated call must carry `Authorization: Token <value>`.

    Pins the wire shape of `_headers()` — a switch to `Bearer` or a
    missing prefix would silently break docker login flows in production.
    """
    route = respx.get(f"{MOCK_API}/api/registry/me/").mock(
        return_value=httpx.Response(200, json={"project": "p"}),
    )
    console.me()
    assert route.called
    assert route.calls.last.request.headers["Authorization"] == "Token test-token"


@respx.mock
def test_request_raises_console_error_on_401(monkeypatched_console_api, with_token):
    """4xx responses must raise ConsoleError carrying status + decoded body."""
    respx.get(f"{MOCK_API}/api/registry/me/").mock(
        return_value=httpx.Response(401, json={"detail": "expired"}),
    )
    with pytest.raises(ConsoleError) as exc:
        console.me()
    assert exc.value.status_code == 401
    assert exc.value.body == {"detail": "expired"}


@respx.mock
def test_request_raises_when_no_token_and_auth_required(monkeypatched_console_api, monkeypatch):
    """`_headers(require=True)` with no token must raise ConsoleError(401)."""
    monkeypatch.setattr(console, "load_api_token", lambda: None)
    with pytest.raises(ConsoleError) as exc:
        console.me()
    assert exc.value.status_code == 401


# -------- Registry: list/check/provision --------

@respx.mock
def test_list_plans(monkeypatched_console_api):
    """GET /api/registry/plans/, no auth required."""
    route = respx.get(f"{MOCK_API}/api/registry/plans/").mock(
        return_value=httpx.Response(200, json=[{"id": 1, "name": "free"}]),
    )
    result = console.list_plans()
    assert route.called
    assert route.calls.last.request.method == "GET"
    assert "Authorization" not in route.calls.last.request.headers
    assert result == [{"id": 1, "name": "free"}]


@respx.mock
def test_check_namespace(monkeypatched_console_api, with_token):
    """GET /api/registry/namespaces/check/?name=... and decode `available`."""
    route = respx.get(f"{MOCK_API}/api/registry/namespaces/check/").mock(
        return_value=httpx.Response(200, json={"available": True}),
    )
    result = console.check_namespace("myorg")
    assert route.called
    assert route.calls.last.request.url.params["name"] == "myorg"
    assert result == {"available": True}


@respx.mock
def test_provision(monkeypatched_console_api, with_token):
    """POST /api/registry/provision/ with `{"namespace": ...}` body."""
    route = respx.post(f"{MOCK_API}/api/registry/provision/").mock(
        return_value=httpx.Response(201, json={"project": "myorg", "robot": "secret"}),
    )
    result = console.provision("myorg")
    assert route.called
    assert route.calls.last.request.method == "POST"
    import json as _json
    body = _json.loads(route.calls.last.request.content)
    assert body == {"namespace": "myorg"}
    assert result == {"project": "myorg", "robot": "secret"}


@respx.mock
def test_provision_status(monkeypatched_console_api, with_token):
    """GET /api/registry/provision/status/."""
    route = respx.get(f"{MOCK_API}/api/registry/provision/status/").mock(
        return_value=httpx.Response(200, json={"status": "ok"}),
    )
    assert console.provision_status() == {"status": "ok"}
    assert route.called


@respx.mock
def test_me(monkeypatched_console_api, with_token):
    """GET /api/registry/me/."""
    route = respx.get(f"{MOCK_API}/api/registry/me/").mock(
        return_value=httpx.Response(200, json={"project": "p"}),
    )
    assert console.me() == {"project": "p"}
    assert route.called


@respx.mock
def test_me_robot(monkeypatched_console_api, with_token):
    """GET /api/registry/me/robot/."""
    route = respx.get(f"{MOCK_API}/api/registry/me/robot/").mock(
        return_value=httpx.Response(200, json={"login": "robot$x"}),
    )
    assert console.me_robot() == {"login": "robot$x"}
    assert route.called


@respx.mock
def test_rotate_robot(monkeypatched_console_api, with_token):
    """POST /api/registry/robot/rotate/ — verb is POST not GET."""
    route = respx.post(f"{MOCK_API}/api/registry/robot/rotate/").mock(
        return_value=httpx.Response(200, json={"secret": "new"}),
    )
    assert console.rotate_robot() == {"secret": "new"}
    assert route.calls.last.request.method == "POST"


# -------- Registry: repositories / artifacts --------

@respx.mock
def test_list_repositories_pagination(monkeypatched_console_api, with_token):
    """GET /api/registry/repositories/?page=2&page_size=10."""
    route = respx.get(f"{MOCK_API}/api/registry/repositories/").mock(
        return_value=httpx.Response(200, json={"results": []}),
    )
    console.list_repositories(page=2, page_size=10)
    p = route.calls.last.request.url.params
    assert p["page"] == "2"
    assert p["page_size"] == "10"


@respx.mock
def test_list_artifacts_routes_to_models_index(monkeypatched_console_api, with_token):
    """`list_artifacts("proj/repo")` hits /api/models/proj/repo/ (NOT registry).

    Routing nuance documented in the source: the registry path returns 405,
    so listing artifacts is served by the model index. A refactor that
    "fixes" this to the registry path would silently break the CLI.
    """
    route = respx.get(f"{MOCK_API}/api/models/proj/repo/").mock(
        return_value=httpx.Response(200, json={"artifacts": [{"tag": "v1"}]}),
    )
    result = console.list_artifacts("proj/repo", page=1, page_size=50)
    assert route.called
    assert result == [{"tag": "v1"}]


@respx.mock
def test_list_artifacts_missing_artifacts_key_returns_empty(monkeypatched_console_api, with_token):
    """Response without `artifacts` key must return []."""
    respx.get(f"{MOCK_API}/api/models/proj/repo/").mock(
        return_value=httpx.Response(200, json={}),
    )
    assert console.list_artifacts("proj/repo") == []


def test_list_artifacts_rejects_bad_repo(monkeypatched_console_api, with_token):
    """`_split_project_repo` raises ValueError on missing slash."""
    with pytest.raises(ValueError, match="<project>/<repo>"):
        console.list_artifacts("no-slash")


@respx.mock
def test_get_artifact(monkeypatched_console_api, with_token):
    """GET /api/models/<proj>/<repo>/<ref>/ via model index."""
    route = respx.get(f"{MOCK_API}/api/models/proj/repo/v1/").mock(
        return_value=httpx.Response(200, json={"tag": "v1"}),
    )
    assert console.get_artifact("proj/repo", "v1") == {"tag": "v1"}
    assert route.called


@respx.mock
def test_delete_artifact(monkeypatched_console_api, with_token):
    """DELETE /api/registry/repositories/<proj>/<repo>/artifacts/<ref>/.

    Note this DOES hit the registry path (not the model index) — opposite
    routing from get_artifact, so a refactor that uses the model-index URL
    would silently 405.
    """
    route = respx.delete(
        f"{MOCK_API}/api/registry/repositories/proj/repo/artifacts/v1/"
    ).mock(return_value=httpx.Response(204))
    console.delete_artifact("proj/repo", "v1")
    assert route.called
    assert route.calls.last.request.method == "DELETE"


@respx.mock
def test_delete_artifact_404_raises(monkeypatched_console_api, with_token):
    """404 on delete must surface as ConsoleError(404)."""
    respx.delete(
        f"{MOCK_API}/api/registry/repositories/proj/repo/artifacts/nope/"
    ).mock(return_value=httpx.Response(404, json={"detail": "not found"}))
    with pytest.raises(ConsoleError) as exc:
        console.delete_artifact("proj/repo", "nope")
    assert exc.value.status_code == 404


# -------- Registry: usage / events / publicity --------

@respx.mock
def test_usage(monkeypatched_console_api, with_token):
    """GET /api/registry/usage/."""
    route = respx.get(f"{MOCK_API}/api/registry/usage/").mock(
        return_value=httpx.Response(200, json={"bytes": 123}),
    )
    assert console.usage() == {"bytes": 123}
    assert route.called


@respx.mock
def test_usage_per_repo(monkeypatched_console_api, with_token):
    """GET /api/registry/usage/repositories/?page=3&page_size=20."""
    route = respx.get(f"{MOCK_API}/api/registry/usage/repositories/").mock(
        return_value=httpx.Response(200, json={"results": []}),
    )
    console.usage_per_repo(page=3, page_size=20)
    p = route.calls.last.request.url.params
    assert p["page"] == "3"
    assert p["page_size"] == "20"


@respx.mock
def test_events(monkeypatched_console_api, with_token):
    """GET /api/registry/events/."""
    route = respx.get(f"{MOCK_API}/api/registry/events/").mock(
        return_value=httpx.Response(200, json=[]),
    )
    assert console.events() == []
    assert route.called


@respx.mock
def test_toggle_publicity(monkeypatched_console_api, with_token):
    """PATCH /api/registry/me/publicity/ with `{"public": bool}`.

    Verb is PATCH not POST/PUT — a verb swap would 405 in prod.
    """
    route = respx.patch(f"{MOCK_API}/api/registry/me/publicity/").mock(
        return_value=httpx.Response(200, json={"public": True}),
    )
    import json as _json
    console.toggle_publicity(True)
    assert route.calls.last.request.method == "PATCH"
    body = _json.loads(route.calls.last.request.content)
    assert body == {"public": True}


# -------- Subscriptions --------

@respx.mock
def test_subscribe_without_upfront(monkeypatched_console_api, with_token):
    """POST /api/registry/subscribe/ with `{"plan_id": ...}` (no pay_upfront)."""
    route = respx.post(f"{MOCK_API}/api/registry/subscribe/").mock(
        return_value=httpx.Response(201, json={"id": 7}),
    )
    import json as _json
    console.subscribe(plan_id=3)
    body = _json.loads(route.calls.last.request.content)
    assert body == {"plan_id": 3}
    assert "pay_upfront" not in body


@respx.mock
def test_subscribe_with_upfront(monkeypatched_console_api, with_token):
    """pay_upfront, when provided, must appear in the body."""
    route = respx.post(f"{MOCK_API}/api/registry/subscribe/").mock(
        return_value=httpx.Response(201, json={"id": 7}),
    )
    import json as _json
    console.subscribe(plan_id=3, pay_upfront=6)
    body = _json.loads(route.calls.last.request.content)
    assert body == {"plan_id": 3, "pay_upfront": 6}


@respx.mock
def test_list_subscriptions(monkeypatched_console_api, with_token):
    """GET /api/registry/subscriptions/."""
    route = respx.get(f"{MOCK_API}/api/registry/subscriptions/").mock(
        return_value=httpx.Response(200, json=[]),
    )
    assert console.list_subscriptions() == []
    assert route.called


@respx.mock
def test_cancel_subscription(monkeypatched_console_api, with_token):
    """DELETE /api/registry/subscriptions/<id>/."""
    route = respx.delete(f"{MOCK_API}/api/registry/subscriptions/42/").mock(
        return_value=httpx.Response(204),
    )
    console.cancel_subscription(42)
    assert route.called
    assert route.calls.last.request.method == "DELETE"


# -------- Per-project API keys --------

@respx.mock
def test_list_keys(monkeypatched_console_api, with_token):
    """GET /api/registry/keys/."""
    route = respx.get(f"{MOCK_API}/api/registry/keys/").mock(
        return_value=httpx.Response(200, json=[]),
    )
    assert console.list_keys() == []
    assert route.called


@respx.mock
def test_create_key_without_expiry(monkeypatched_console_api, with_token):
    """POST /api/registry/keys/ with name + role; expires_days omitted."""
    route = respx.post(f"{MOCK_API}/api/registry/keys/").mock(
        return_value=httpx.Response(201, json={"id": 1, "secret": "abc"}),
    )
    import json as _json
    console.create_key("ci-key", "developer")
    body = _json.loads(route.calls.last.request.content)
    assert body == {"name": "ci-key", "role": "developer"}


@respx.mock
def test_create_key_with_expiry(monkeypatched_console_api, with_token):
    """expires_days, when set, is included in the body."""
    route = respx.post(f"{MOCK_API}/api/registry/keys/").mock(
        return_value=httpx.Response(201, json={"id": 1}),
    )
    import json as _json
    console.create_key("ci-key", "developer", expires_days=30)
    body = _json.loads(route.calls.last.request.content)
    assert body == {"name": "ci-key", "role": "developer", "expires_days": 30}


@respx.mock
def test_show_key(monkeypatched_console_api, with_token):
    """GET /api/registry/keys/<id>/."""
    route = respx.get(f"{MOCK_API}/api/registry/keys/5/").mock(
        return_value=httpx.Response(200, json={"id": 5}),
    )
    assert console.show_key(5) == {"id": 5}
    assert route.called


@respx.mock
def test_rotate_key(monkeypatched_console_api, with_token):
    """POST /api/registry/keys/<id>/rotate/ — verb POST."""
    route = respx.post(f"{MOCK_API}/api/registry/keys/5/rotate/").mock(
        return_value=httpx.Response(200, json={"secret": "new"}),
    )
    assert console.rotate_key(5) == {"secret": "new"}
    assert route.calls.last.request.method == "POST"


@respx.mock
def test_revoke_key(monkeypatched_console_api, with_token):
    """DELETE /api/registry/keys/<id>/."""
    route = respx.delete(f"{MOCK_API}/api/registry/keys/5/").mock(
        return_value=httpx.Response(204),
    )
    console.revoke_key(5)
    assert route.calls.last.request.method == "DELETE"


# -------- Model index (no auth) --------

@respx.mock
def test_models_list_passes_filters(monkeypatched_console_api):
    """GET /api/models/ with each filter mapped to its query-param name.

    Pins the param-name mapping: `fmt` -> `format`, `mine=True` -> "true".
    A typo in the dict-builder would silently send the wrong facet.
    """
    route = respx.get(f"{MOCK_API}/api/models/").mock(
        return_value=httpx.Response(200, json={"results": []}),
    )
    console.models_list(
        fmt="gguf", architecture="llama", quantization="q4_k_m",
        min_params=1000000, max_params=10000000, q="needle", mine=True,
        page=2, page_size=5,
    )
    p = route.calls.last.request.url.params
    assert p["format"] == "gguf"
    assert p["architecture"] == "llama"
    assert p["quantization"] == "q4_k_m"
    assert p["min_params"] == "1000000"
    assert p["max_params"] == "10000000"
    assert p["q"] == "needle"
    assert p["mine"] == "true"
    assert p["page"] == "2"
    assert p["page_size"] == "5"
    assert "Authorization" not in route.calls.last.request.headers


@respx.mock
def test_models_formats(monkeypatched_console_api):
    """GET /api/models/formats/, anonymous."""
    route = respx.get(f"{MOCK_API}/api/models/formats/").mock(
        return_value=httpx.Response(200, json={"formats": ["gguf"]}),
    )
    assert console.models_formats() == {"formats": ["gguf"]}
    assert "Authorization" not in route.calls.last.request.headers


@respx.mock
def test_model_repo(monkeypatched_console_api):
    """GET /api/models/<proj>/<repo>/, anonymous."""
    route = respx.get(f"{MOCK_API}/api/models/proj/repo/").mock(
        return_value=httpx.Response(200, json={"name": "repo"}),
    )
    assert console.model_repo("proj", "repo") == {"name": "repo"}
    assert "Authorization" not in route.calls.last.request.headers


@respx.mock
def test_model_detail(monkeypatched_console_api):
    """GET /api/models/<proj>/<repo>/<ref>/, anonymous."""
    route = respx.get(f"{MOCK_API}/api/models/proj/repo/v1/").mock(
        return_value=httpx.Response(200, json={"tag": "v1"}),
    )
    assert console.model_detail("proj", "repo", "v1") == {"tag": "v1"}
    assert "Authorization" not in route.calls.last.request.headers


@respx.mock
def test_model_detail_404_raises(monkeypatched_console_api):
    """404 from the model index must surface as ConsoleError(404)."""
    respx.get(f"{MOCK_API}/api/models/proj/repo/missing/").mock(
        return_value=httpx.Response(404, json={"detail": "no such ref"}),
    )
    with pytest.raises(ConsoleError) as exc:
        console.model_detail("proj", "repo", "missing")
    assert exc.value.status_code == 404


# -------- Token persistence helpers --------

def test_save_and_load_api_token(tmp_path, monkeypatch):
    """save_api_token persists 0o600; load_api_token round-trips the value."""
    import os
    token_path = tmp_path / "api_token"
    monkeypatch.setattr(console, "DEFAULT_CACHE_DIR", str(tmp_path))
    monkeypatch.setattr(console, "API_TOKEN_PATH", str(token_path))
    # auth._atomic_write_secret reads its target path from the second arg,
    # so we don't need to patch auth itself; just pass the patched path
    # via the console module's binding.
    console.save_api_token("  my-token\n")  # whitespace stripped per source
    assert console.load_api_token() == "my-token"
    # 0o600 invariant pinned by RACE-1; this test is the consumer-side check.
    mode = os.stat(token_path).st_mode & 0o777
    assert mode == 0o600


def test_load_api_token_returns_none_when_missing(tmp_path, monkeypatch):
    """Absent token file must return None, not raise."""
    monkeypatch.setattr(console, "API_TOKEN_PATH", str(tmp_path / "nope"))
    assert console.load_api_token() is None
