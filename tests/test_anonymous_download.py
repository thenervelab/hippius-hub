"""End-to-end test for audit C2: `token=False` is the HF anonymous sentinel.

The C2 fix routes `token=False` to `get_oci_bearer_token` with `False`
preserved, which:
  - skips the ~/.docker/config.json fallback (a confused-deputy elevation
    risk — a user asking for anonymous I/O must not silently push under
    their docker creds)
  - sends the /service/token request with NO Authorization header

This file pins both invariants end-to-end through `hf_hub_download`,
using respx for the Python-side HTTP and a monkeypatched
`download_file_native` so the Rust blob fetch doesn't need a real socket
(the C2 surface is the token-service handshake, not the blob bytes).
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


def _valid_jwt() -> str:
    def b64(x):
        return base64.urlsafe_b64encode(json.dumps(x).encode()).decode().rstrip("=")
    return f"{b64({'alg': 'none'})}.{b64({'exp': int(time.time()) + 3600})}.signature"


@pytest.fixture(autouse=True)
def _point_registry_at_mock(monkeypatch):
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_REGISTRY_URL", REGISTRY)
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_REGISTRY_URL", REGISTRY)


@pytest.fixture
def stub_blob_download(monkeypatch):
    """Replace download_file_native so the Rust extension isn't exercised.

    The C2 surface is the token-service request shape — not the bytes
    that come back from the blob URL. Stubbing the native call lets us
    keep the test fully respx-driven without needing a real localhost
    server. Yields the list of dest paths the stub was asked to fill, so
    a test can assert it was reached (otherwise we'd silently pass if
    `hf_hub_download` short-circuited before the download).
    """
    fills: list[str] = []

    def fake_download(*, url, dest_path, auth_token, chunk_size, verify_hash):
        fills.append(dest_path)
        Path(dest_path).write_bytes(b"x")
        return None  # verify_hash=False sentinel

    monkeypatch.setattr("hippius_hub.file_download.download_file_native", fake_download)
    return fills


@pytest.fixture
def block_docker_auth(monkeypatch):
    """Make get_docker_auth raise if called.

    This is the load-bearing part of C2: a caller asking for anonymous
    I/O must not be silently elevated to their docker-stored creds.
    Raising here turns "anonymous accidentally consulted docker config"
    into a test failure, even if the test's respx routes would have
    forgiven a Basic-Auth header.
    """
    def must_not_be_called(_registry_url):
        raise AssertionError(
            "get_docker_auth was consulted while token=False — that would "
            "elevate the request under ambient docker creds, defeating C2."
        )
    monkeypatch.setattr("hippius_hub.auth.get_docker_auth", must_not_be_called)


def _stub_token_manifest(blob_sha_hex: str) -> respx.routes.Route:
    """Wire token/manifest/blob-head routes. Returns the token route so
    the caller can inspect its `.calls.last.request.headers`."""
    token_route = respx.mock.get(
        url__startswith=f"{REGISTRY}/service/token",
    ).mock(return_value=httpx.Response(200, json={"token": _valid_jwt()}))

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
    return token_route


@respx.mock
def test_token_false_sends_no_authorization_header(
    tmp_path, stub_blob_download, block_docker_auth,
):
    """`hf_hub_download(token=False)` must:

    1. Reach the token-service endpoint
    2. Send NO Authorization header on that request
    3. NOT consult get_docker_auth (pinned by block_docker_auth fixture)
    """
    from hippius_hub import hf_hub_download

    blob_sha = hashlib.sha256(b"x").hexdigest()
    token_route = _stub_token_manifest(blob_sha)

    hf_hub_download(
        repo_id=REPO_ID,
        filename="model.bin",
        revision=REVISION,
        cache_dir=str(tmp_path),
        token=False,
    )

    assert token_route.called, "token-service endpoint was never reached"
    # The behavioral pin: token=False → no Authorization header on the
    # token-service request. A regression that collapsed False to None
    # would silently fall through to the docker fallback, which we'd
    # catch via block_docker_auth — but a regression that emitted a
    # blank or "Bearer " header would slip past that check, so we also
    # assert directly.
    sent_headers = token_route.calls.last.request.headers
    assert "authorization" not in {k.lower() for k in sent_headers.keys()}, (
        f"token=False must produce no Authorization header; got: "
        f"{dict(sent_headers)!r}"
    )

    # And the blob fetch was actually reached — otherwise the test could
    # have passed by short-circuiting before the auth handshake.
    assert len(stub_blob_download) == 1, (
        f"expected exactly one blob download, got {stub_blob_download!r}"
    )


@respx.mock
def test_token_none_does_consult_docker_auth(tmp_path, monkeypatch, stub_blob_download):
    """Sanity counter-case: `token=None` (no caller preference) MUST still
    consult get_docker_auth — that's the documented behavior in
    auth.py:280-285. If this regressed to "never consult docker",
    docker-logged-in users would silently fall to anonymous and 401.
    """
    from hippius_hub import hf_hub_download

    docker_auth_calls: list[str] = []

    def stub_docker(registry_url):
        docker_auth_calls.append(registry_url)
        return None  # no creds saved — fall through to anonymous

    monkeypatch.setattr("hippius_hub.auth.get_docker_auth", stub_docker)

    blob_sha = hashlib.sha256(b"x").hexdigest()
    _stub_token_manifest(blob_sha)

    hf_hub_download(
        repo_id=REPO_ID,
        filename="model.bin",
        revision=REVISION,
        cache_dir=str(tmp_path),
        token=None,
    )

    assert docker_auth_calls, (
        "get_docker_auth was NOT consulted under token=None; that breaks "
        "the documented three-state semantics (None means 'try docker fallback')"
    )
