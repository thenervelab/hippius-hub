"""INPUT-1: `endpoint` is a credential trust boundary.

Two guarantees:

1. The OCI bearer token is minted from the *same* origin it will be sent to
   (``get_oci_bearer_token`` honors ``endpoint``), so a Hippius-issued token
   can't be harvested by pointing at an attacker endpoint, and the token cache
   is keyed per-origin (no cross-origin bleed).
2. A stored/ambient credential (``token=None``/``True``, read from the saved
   login file) is never forwarded to a non-default endpoint — an explicit
   ``token="..."`` is the caller's own credential and is allowed through.

Pure unit tests (respx); no real network.
"""
import base64
import json
import time

import httpx
import pytest
import respx

from hippius_hub import auth
from hippius_hub.auth import (
    clear_oci_token_cache,
    get_oci_bearer_token,
    resolve_auth_header,
)
from hippius_hub.constants import DEFAULT_REGISTRY_URL

OTHER = "https://other-registry.test.invalid"


def _jwt(exp_ts: int) -> str:
    def b64(x):
        return base64.urlsafe_b64encode(json.dumps(x).encode()).decode().rstrip("=")
    return f"{b64({'alg': 'none'})}.{b64({'exp': exp_ts})}.sig"


@pytest.fixture(autouse=True)
def _clear():
    clear_oci_token_cache()
    yield
    clear_oci_token_cache()


@respx.mock
def test_bearer_minted_at_requested_endpoint():
    fresh = _jwt(int(time.time()) + 3600)
    other = respx.mock.get(url__startswith=f"{OTHER}/service/token").mock(
        return_value=httpx.Response(200, json={"token": fresh})
    )
    default = respx.mock.get(url__startswith=f"{DEFAULT_REGISTRY_URL}/service/token").mock(
        return_value=httpx.Response(200, json={"token": "DEFAULT-MINTED"})
    )

    result = get_oci_bearer_token("foo/bar", token=False, use_cache=False, endpoint=OTHER)

    assert other.called, "token must be minted at the requested endpoint"
    assert not default.called, "must NOT mint at the default registry for a custom endpoint"
    assert result == fresh


@respx.mock
def test_token_cache_keyed_per_origin():
    fresh = _jwt(int(time.time()) + 3600)
    default = respx.mock.get(url__startswith=f"{DEFAULT_REGISTRY_URL}/service/token").mock(
        return_value=httpx.Response(200, json={"token": fresh})
    )
    other = respx.mock.get(url__startswith=f"{OTHER}/service/token").mock(
        return_value=httpx.Response(200, json={"token": fresh})
    )

    get_oci_bearer_token("foo/bar", token=False, use_cache=True, endpoint=None)
    get_oci_bearer_token("foo/bar", token=False, use_cache=True, endpoint=OTHER)

    assert default.called and other.called, (
        "same (repo, token) at two origins must not share a cache slot — a "
        "default-minted token must never be reused against a custom endpoint"
    )


def test_ambient_creds_refused_for_nondefault_endpoint(tmp_path, monkeypatch):
    monkeypatch.setattr(auth, "TOKEN_PATH", str(tmp_path / "token"))
    auth.login(username="u", password="p")  # stores "Basic ..."

    with pytest.raises(ValueError, match="(?i)non-default|off-origin|stored|ambient"):
        resolve_auth_header(None, endpoint=OTHER)


def test_explicit_token_allowed_for_nondefault_endpoint():
    # The caller's own credential — they chose to send it; allowed.
    assert resolve_auth_header("my-explicit-token", endpoint=OTHER) == "Bearer my-explicit-token"


def test_ambient_creds_allowed_for_default_endpoint(tmp_path, monkeypatch):
    monkeypatch.setattr(auth, "TOKEN_PATH", str(tmp_path / "token"))
    auth.login(username="u", password="p")

    header = resolve_auth_header(None)  # default endpoint
    assert header.startswith("Basic ")


@respx.mock
def test_get_oci_bearer_refuses_ambient_stored_creds_offorigin(tmp_path, monkeypatch):
    """The OCI bearer path must also refuse to forward stored creds off-origin —
    not just the Harbor-admin resolve_auth_header path."""
    monkeypatch.setattr(auth, "TOKEN_PATH", str(tmp_path / "token"))
    auth.login(username="u", password="p")
    # No route registered: a correct implementation raises before any request.
    with pytest.raises(ValueError, match="(?i)non-default|off-origin|stored|ambient"):
        get_oci_bearer_token("foo/bar", token=None, use_cache=False, endpoint=OTHER)
