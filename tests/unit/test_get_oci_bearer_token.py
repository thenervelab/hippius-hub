"""Behavioral respx-based unit tests for get_oci_bearer_token.

Uses the respx fixtures from tests/respx_fixtures.py (Task 2.4 wiring)
to mock the /service/token endpoint without any real network. Pins:
- Cache hit avoids the network round-trip.
- Cache miss issues the request with the correct scope.
- Token=False (post-Task-1.2) does NOT trigger the docker fallback.
- Timeout kwarg is passed (post-Task-1.1).
- JWT exp parse failure leaves the cache unpopulated (post-Phase-4.2).
"""
import base64
import json
import time
import httpx
import pytest
import respx

from hippius_hub.auth import (
    get_oci_bearer_token,
    clear_oci_token_cache,
    _OCI_TOKEN_CACHE,
)


def _jwt_with_exp(exp_ts: int) -> str:
    """Build a 3-segment JWT whose payload contains the given exp claim."""
    def b64(x):
        return base64.urlsafe_b64encode(json.dumps(x).encode()).decode().rstrip("=")
    return f"{b64({'alg': 'none'})}.{b64({'exp': exp_ts})}.signature"


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_oci_token_cache()
    yield
    clear_oci_token_cache()


@respx.mock
def test_cache_miss_issues_request(monkeypatched_registry):
    from tests.respx_fixtures import token_route, MOCK_REGISTRY
    fresh = _jwt_with_exp(int(time.time()) + 3600)
    route = token_route(respx.mock, scope="repository:foo/bar:pull", token_value=fresh)
    result = get_oci_bearer_token("foo/bar", token=False, use_cache=False)
    assert result == fresh
    assert route.called


@respx.mock
def test_cache_hit_skips_network(monkeypatched_registry):
    from tests.respx_fixtures import token_route, MOCK_REGISTRY
    fresh = _jwt_with_exp(int(time.time()) + 3600)
    route = token_route(respx.mock, scope="repository:foo/bar:pull", token_value=fresh)
    first = get_oci_bearer_token("foo/bar", token=False, use_cache=True)
    second = get_oci_bearer_token("foo/bar", token=False, use_cache=True)
    assert first == second == fresh
    assert route.call_count == 1, "second call must hit cache, not the network"


@respx.mock
def test_token_false_skips_docker_fallback(monkeypatched_registry, monkeypatch):
    """Audit C2 regression: token=False MUST NOT consult ~/.docker/config.json."""
    from tests.respx_fixtures import token_route
    fresh = _jwt_with_exp(int(time.time()) + 3600)
    token_route(respx.mock, scope="repository:foo/bar:pull", token_value=fresh)

    docker_calls = []
    def fake_docker_auth(url):
        docker_calls.append(url)
        return "STOLEN"
    monkeypatch.setattr("hippius_hub.auth.get_docker_auth", fake_docker_auth)

    get_oci_bearer_token("foo/bar", token=False, use_cache=False)
    assert docker_calls == [], "token=False must bypass get_docker_auth entirely"


@respx.mock
def test_unparseable_jwt_does_not_cache(monkeypatched_registry):
    """JWT with no exp claim (parse failure) must NOT populate the cache."""
    from tests.respx_fixtures import token_route
    bad_jwt = "h.@@notbase64@@.s"  # _jwt_expiration returns None + warns
    token_route(respx.mock, scope="repository:foo/bar:pull", token_value=bad_jwt)
    import warnings as w
    with w.catch_warnings():
        w.simplefilter("ignore")  # M4 warning is expected
        result = get_oci_bearer_token("foo/bar", token=False, use_cache=True)
    assert result == bad_jwt
    # Cache must be empty (parse failure short-circuits the cache write).
    assert _OCI_TOKEN_CACHE == {}, "unparseable JWT must not populate the cache"


@respx.mock
def test_request_has_timeout(monkeypatched_registry):
    """Audit C1 regression: the GET must pass timeout= (verified by inspect)."""
    import inspect
    from hippius_hub import auth
    src = inspect.getsource(auth.get_oci_bearer_token)
    assert "timeout=" in src, "get_oci_bearer_token must pass timeout= to httpx"
