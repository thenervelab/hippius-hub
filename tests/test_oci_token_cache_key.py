"""End-to-end test for audit M3: OCI token cache keys on hashed token.

Pre-M3 the cache key was `(repo_id, push)` — two users hitting the same
repo with different tokens would share the SAME cache entry, meaning
the second user got the first user's bearer JWT. M3 added the token
hash to the key (auth.py:_token_cache_key) so divergent tokens map to
divergent cache entries.

The existing unit tests in tests/unit/test_get_oci_bearer_token.py
cover the cache key in isolation. This file is the integration
counterpart: drive the full `hf_hub_download` path twice with two
different tokens against the same repo+revision, and assert the
token-service endpoint saw TWO requests with two different
Authorization headers.

A regression that reverted the cache key to (repo_id, push) would
silently surface as the second call returning the cached JWT from the
first call — token_route.call_count would be 1, not 2.

The autouse cache-clear in tests/conftest.py:18 fires between tests but
NOT within a test. Both calls in each test below run inside a single
pytest function, so the cache state at the start (empty, from the
post-test clear of whichever test ran before) is identical to a fresh
process — and the within-test behavior is what M3 actually pins.
"""
from __future__ import annotations

import base64
import hashlib
import json
import time
from pathlib import Path

import httpx
import pytest
import respx


REGISTRY = "https://registry.hippius.test"
REPO_ID = "owner/repo"
REVISION = "main"


def _valid_jwt(name: str = "jwt") -> str:
    """JWT whose payload encodes a per-test identity in `sub` and an
    exp far enough in the future to be cacheable. The two calls under
    test fetch distinct values; the name lets a failure message pin
    which token was leaked into the wrong slot.
    """
    def b64(x):
        return base64.urlsafe_b64encode(json.dumps(x).encode()).decode().rstrip("=")
    return f"{b64({'alg': 'none'})}.{b64({'exp': int(time.time()) + 3600, 'sub': name})}.signature"


@pytest.fixture(autouse=True)
def _point_registry_at_mock(monkeypatch):
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_REGISTRY_URL", REGISTRY)
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_REGISTRY_URL", REGISTRY)


@pytest.fixture
def stub_blob_download(monkeypatch):
    """Stub the Rust download so the test only exercises Python orchestration."""
    def fake_download(*, url, dest_path, auth_token, chunk_size, verify_hash, content_length=None):
        Path(dest_path).write_bytes(b"x")
        return None
    monkeypatch.setattr("hippius_hub.file_download.download_file_native", fake_download)


def _stub_manifest_routes(blob_sha_hex: str) -> None:
    respx.mock.get(f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}").mock(
        return_value=httpx.Response(
            200,
            json={
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "config": {
                    "mediaType": "application/vnd.oci.empty.v1+json",
                    "digest": "sha256:" + hashlib.sha256(b"{}").hexdigest(),
                    "size": 2,
                },
                "layers": [
                    {
                        "mediaType": "application/vnd.oci.image.layer.v1.tar",
                        "digest": f"sha256:{blob_sha_hex}",
                        "size": 1,
                        "annotations": {
                            "org.opencontainers.image.title": "model.bin",
                        },
                    }
                ],
            },
            headers={"Docker-Content-Digest": "sha256:" + "a" * 64},
        )
    )


@respx.mock
def test_two_different_tokens_hit_token_endpoint_twice(tmp_path, stub_blob_download):
    """Two distinct tokens, one repo: token service must receive 2 GETs
    with the corresponding Authorization headers. A regression that
    reverted to a `(repo_id, push)` cache key would only see 1 GET (the
    second call would short-circuit on the cached JWT from the first).
    """
    from hippius_hub import hf_hub_download

    token_a_jwt = _valid_jwt("user-a")
    token_b_jwt = _valid_jwt("user-b")
    issued: list[str] = []

    def issue_token(request):
        issued.append(request.headers.get("authorization", "<none>"))
        # The fake JWT body must be valid (parseable) so it gets cached —
        # otherwise _jwt_expiration returns None and caching is skipped,
        # which would mask any cache-key bug.
        if "Bearer literal-user-a" in request.headers.get("authorization", ""):
            return httpx.Response(200, json={"token": token_a_jwt})
        return httpx.Response(200, json={"token": token_b_jwt})

    token_route = respx.mock.get(
        url__startswith=f"{REGISTRY}/service/token",
    ).mock(side_effect=issue_token)

    blob_sha = hashlib.sha256(b"x").hexdigest()
    _stub_manifest_routes(blob_sha)

    # First call: user A
    hf_hub_download(
        repo_id=REPO_ID,
        filename="model.bin",
        revision=REVISION,
        cache_dir=str(tmp_path / "cache-a"),
        token="literal-user-a",
    )
    # Second call: user B, same repo+revision
    hf_hub_download(
        repo_id=REPO_ID,
        filename="model.bin",
        revision=REVISION,
        cache_dir=str(tmp_path / "cache-b"),
        token="literal-user-b",
    )

    assert token_route.call_count == 2, (
        f"expected 2 token-service GETs (one per distinct token), got "
        f"{token_route.call_count}. Regression: cache key probably "
        f"reverted to (repo_id, push) and the second call short-circuited "
        f"on user-a's cached JWT.\nObserved Authorization headers: {issued!r}"
    )
    assert "Bearer literal-user-a" in issued[0]
    assert "Bearer literal-user-b" in issued[1]


@respx.mock
def test_same_token_twice_hits_token_endpoint_once(tmp_path, stub_blob_download):
    """Same token, same repo: cache MUST hit. Pin the positive case so a
    regression that broadened the cache key (e.g. to include time-bucket)
    and broke caching entirely doesn't slip past.
    """
    from hippius_hub import hf_hub_download

    token_jwt = _valid_jwt("user-x")
    token_route = respx.mock.get(
        url__startswith=f"{REGISTRY}/service/token",
    ).mock(return_value=httpx.Response(200, json={"token": token_jwt}))

    blob_sha = hashlib.sha256(b"x").hexdigest()
    _stub_manifest_routes(blob_sha)

    for cache_suffix in ("call-a", "call-b"):
        hf_hub_download(
            repo_id=REPO_ID,
            filename="model.bin",
            revision=REVISION,
            cache_dir=str(tmp_path / cache_suffix),
            token="literal-user-x",
        )

    assert token_route.call_count == 1, (
        f"expected exactly 1 token-service GET (same token → cache hit), "
        f"got {token_route.call_count}. Regression: cache key probably "
        f"gained per-call entropy (e.g. instant time-bucket) — caching "
        f"is now ineffective."
    )


@respx.mock
def test_token_false_does_not_share_cache_with_literal_token(tmp_path, stub_blob_download, monkeypatch):
    """The HF anonymous sentinel (False) must have its own cache slot.
    A regression that collapsed `False` to `None` in the cache key would
    silently return a cached anonymous JWT when a literal token was
    requested — a privilege-downgrade bug.
    """
    from hippius_hub import hf_hub_download
    # Suppress docker-auth fallback so the token=None call doesn't
    # consult ~/.docker/config.json and skew the test.
    monkeypatch.setattr("hippius_hub.auth.get_docker_auth", lambda _u: None)

    anon_jwt = _valid_jwt("anonymous")
    literal_jwt = _valid_jwt("literal-bearer")

    def issue_token(request):
        if "authorization" in {k.lower() for k in request.headers.keys()}:
            return httpx.Response(200, json={"token": literal_jwt})
        return httpx.Response(200, json={"token": anon_jwt})

    token_route = respx.mock.get(
        url__startswith=f"{REGISTRY}/service/token",
    ).mock(side_effect=issue_token)

    blob_sha = hashlib.sha256(b"x").hexdigest()
    _stub_manifest_routes(blob_sha)

    # Anonymous call first
    hf_hub_download(
        repo_id=REPO_ID, filename="model.bin", revision=REVISION,
        cache_dir=str(tmp_path / "anon"), token=False,
    )
    # Literal-token call second — must not reuse the anonymous cache slot
    hf_hub_download(
        repo_id=REPO_ID, filename="model.bin", revision=REVISION,
        cache_dir=str(tmp_path / "lit"), token="literal-bearer",
    )

    assert token_route.call_count == 2, (
        f"anonymous and literal-token calls share a cache slot — that's "
        f"a privilege-downgrade. token endpoint saw {token_route.call_count} "
        f"call(s), expected 2."
    )
