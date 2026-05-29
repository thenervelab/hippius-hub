"""Regression: OCI bearer cache must hash the token, not store it raw."""
import inspect
from hippius_hub import auth


def test_get_oci_bearer_token_does_not_store_raw_token_in_cache():
    src = inspect.getsource(auth.get_oci_bearer_token)
    # The raw token must NOT be used as the cache key directly.
    assert "cache_key = (repo_id, push, token)" not in src, (
        "_OCI_TOKEN_CACHE must key on a hash of the token, not the raw value"
    )
    assert "_token_cache_key" in src, (
        "Use the _token_cache_key helper that hashes the token"
    )


REG = "https://r.example"


def test_token_cache_key_hashes_string_token():
    key = auth._token_cache_key("foo/bar", False, "secret-token", REG)
    repo, push, hashed, registry = key
    assert repo == "foo/bar"
    assert push is False
    assert registry == REG
    # SHA-256 hex is 64 chars.
    assert len(hashed) == 64 and all(c in "0123456789abcdef" for c in hashed)
    assert hashed != "secret-token"


def test_token_cache_key_handles_none():
    key = auth._token_cache_key("foo/bar", False, None, REG)
    assert key == ("foo/bar", False, None, REG)


def test_token_cache_key_distinguishes_anonymous_from_unset():
    """token=False (explicit anon, per Task 1.2) must produce a distinct key
    from token=None (unset/use-saved) so the cache doesn't conflate them."""
    anon = auth._token_cache_key("foo/bar", False, False, REG)
    unset = auth._token_cache_key("foo/bar", False, None, REG)
    assert anon != unset


def test_token_cache_key_distinguishes_registry():
    """Same (repo, token) at two origins must produce distinct keys (INPUT-1):
    a token minted by the default registry must never be served from cache for
    a request aimed at a different endpoint."""
    a = auth._token_cache_key("foo/bar", False, "tok", "https://a.example")
    b = auth._token_cache_key("foo/bar", False, "tok", "https://b.example")
    assert a != b
