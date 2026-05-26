"""Reusable respx mock surfaces for the Hippius OCI registry.

Phase 2.1's `test_upload_if_match.py` had to hand-roll ~30 lines of route
plumbing (token endpoint, manifest GET/PUT, blob HEAD/PUT, plus the
dual-patch of `constants.DEFAULT_REGISTRY_URL` AND `auth.DEFAULT_REGISTRY_URL`)
to keep the test fully offline. Phase 6's unit-test backfill for `auth.py`,
`_oci.py`, and `_repo_ops.py` needs the same plumbing — extracting it once
here keeps every test focused on the BEHAVIOR it is asserting instead of
the registry handshake required to get there.

Why a non-resolvable TLD: `MOCK_REGISTRY` uses `.test.invalid` so any test
that forgets to call `monkeypatched_registry` (or any production code that
slips through respx) gets an immediate DNS failure rather than silently
hitting the real registry. RFC 6761 reserves `.invalid` exactly for this
"must never resolve" purpose.

Why both modules get patched: `auth.py` did `from .constants import
DEFAULT_REGISTRY_URL` at import time, which copies the value into the
`hippius_hub.auth` namespace. Patching only `constants.DEFAULT_REGISTRY_URL`
leaves `auth.DEFAULT_REGISTRY_URL` pointing at the production registry and
the token-service GET escapes the mock. Task 2.1 discovered this the hard
way; the `monkeypatched_registry` fixture below encapsulates the workaround
so no Phase 6 test has to rediscover it.
"""
from __future__ import annotations

import hashlib
from typing import Optional

import httpx
import pytest
import respx


MOCK_REGISTRY = "https://registry.test.invalid"
"""Canonical mock registry URL used by every fixture in this module.

`.test.invalid` is RFC 6761-reserved as never-resolvable, so accidental
real-network hits fail loudly with DNS errors instead of silently leaking
to the real production registry. Any test wiring should use this constant
rather than literal URLs so a future move to a different mock host is a
one-line change here.
"""

DEFAULT_TOKEN = "mock-jwt-token"
"""Default token returned by `token_route` when callers don't override it.

A bare string (no real JWT structure) is fine for most tests: the token
is only used as a bearer-header value downstream. Tests that exercise
`_jwt_expiration` or the cache-TTL path need to pass a real three-segment
JWT via `token_value=` so the `exp` claim is parseable.
"""

EMPTY_CONFIG_DIGEST = "sha256:" + hashlib.sha256(b"{}").hexdigest()
"""Digest of the canonical empty-config blob (`{}`) every OCI manifest needs.

`_ensure_config_blob_uploaded` always HEADs this digest before writing a
manifest. Pre-computing it here lets fixtures stub the HEAD with a 200
without each test re-hashing the same two bytes.
"""


def token_route(
    mock_router: respx.MockRouter,
    *,
    scope: Optional[str] = None,
    token_value: str = DEFAULT_TOKEN,
    status: int = 200,
):
    """Register a respx route for the Hippius `/service/token` endpoint.

    `auth.get_oci_bearer_token` builds the URL as
    `{REGISTRY}/service/token?service=harbor-registry&scope=...` — using
    `url__startswith` keeps the route match independent of query-string
    ordering, which httpx does not guarantee. The `scope` parameter is
    accepted for documentation but currently ignored in matching; pass it
    to communicate intent in the test source.

    Returns the `Route` so callers can assert on `route.called` /
    `route.calls.last.request.headers` to verify Authorization plumbing.
    """
    del scope  # documentary only — match-on-scope would over-constrain the route
    if status == 200:
        response = httpx.Response(status, json={"token": token_value})
    else:
        response = httpx.Response(status)
    return mock_router.get(url__startswith=f"{MOCK_REGISTRY}/service/token").mock(
        return_value=response
    )


def manifest_get_route(
    mock_router: respx.MockRouter,
    repo: str,
    revision: str,
    *,
    layers: Optional[list] = None,
    digest: str = "sha256:" + "a" * 64,
    missing: bool = False,
    omit_digest_header: bool = False,
):
    """Register a respx route for `GET /v2/{repo}/manifests/{revision}`.

    Three modes:

    - `missing=True`: returns 404 (exercises `fetch_manifest(missing_ok=True)`
      and the `_prev_digest_or_warn(None)` "fresh repo" path).
    - `omit_digest_header=True`: returns 200 with a body but no
      `Docker-Content-Digest` header (exercises the Task 2.1 C1 follow-up:
      `_prev_digest_or_warn` must emit a UserWarning and proceed without
      If-Match because the prior digest is unknown).
    - default: returns 200 with a minimal valid manifest and the
      `Docker-Content-Digest: {digest}` header so callers exercising the
      happy path get a digest they can assert on.

    Callers needing a richer manifest body (specific layers, custom config
    digest, alternate media type) can pass `layers=[...]` — anything more
    bespoke than that should set the route up inline rather than extending
    this fixture into a manifest-builder DSL.
    """
    url = f"{MOCK_REGISTRY}/v2/{repo}/manifests/{revision}"
    if missing:
        return mock_router.get(url).mock(return_value=httpx.Response(404))

    manifest_body = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": EMPTY_CONFIG_DIGEST,
            "size": 2,
        },
        "layers": layers if layers is not None else [],
    }
    headers = {} if omit_digest_header else {"Docker-Content-Digest": digest}
    return mock_router.get(url).mock(
        return_value=httpx.Response(200, json=manifest_body, headers=headers)
    )


def manifest_put_route(
    mock_router: respx.MockRouter,
    repo: str,
    revision: str,
    *,
    status: int = 201,
    digest: str = "sha256:" + "b" * 64,
):
    """Register a respx route for `PUT /v2/{repo}/manifests/{revision}`.

    Default `status=201` simulates a successful manifest write; pass
    `status=412` to drive the `ConcurrentManifestUpdateError` path in
    `_put_manifest` (which checks for 412 specifically before
    `raise_for_status()`).

    The 412 response intentionally still carries the `Docker-Content-Digest`
    header — registries that surface the conflict often echo the current
    server-side digest so the caller can refetch and retry with the right
    If-Match. Tests that care about that header are welcome to assert on it.
    """
    return mock_router.put(f"{MOCK_REGISTRY}/v2/{repo}/manifests/{revision}").mock(
        return_value=httpx.Response(status, headers={"Docker-Content-Digest": digest})
    )


def blob_head_route(
    mock_router: respx.MockRouter,
    repo: str,
    digest: str,
    *,
    exists: bool = True,
):
    """Register `HEAD /v2/{repo}/blobs/{digest}` returning 200 (exists) or 404.

    `_ensure_blob_uploaded` HEADs the digest before posting an upload init;
    a 200 short-circuits the upload (the blob is already in the registry).
    Most upload tests want `exists=True` so they don't have to mock the
    full POST /uploads/ + PUT /uploads/<uuid> dance — see `blob_put_route`
    for that path.
    """
    status = 200 if exists else 404
    return mock_router.head(f"{MOCK_REGISTRY}/v2/{repo}/blobs/{digest}").mock(
        return_value=httpx.Response(status)
    )


def blob_put_route(
    mock_router: respx.MockRouter,
    repo: str,
    *,
    status: int = 201,
    upload_uuid: str = "fixture-upload-uuid",
):
    """Stub the two-step blob upload: `POST /uploads/` then `PUT /uploads/<uuid>`.

    Returns the PUT route (the one tests typically assert on for digest /
    Authorization headers). The POST init returns a `Location` header
    pointing at `{MOCK_REGISTRY}/v2/{repo}/blobs/uploads/{upload_uuid}`,
    which `_ensure_blob_uploaded` will then suffix with `?digest=<digest>`
    and PUT to. We match the PUT with `url__startswith` because that query
    string is computed by the caller and varies per blob.

    NB: this fixture is only needed when `blob_head_route(..., exists=False)`
    forces the upload path. The happy-path "blob already there" tests can
    skip it entirely.
    """
    upload_url = f"{MOCK_REGISTRY}/v2/{repo}/blobs/uploads/{upload_uuid}"
    mock_router.post(f"{MOCK_REGISTRY}/v2/{repo}/blobs/uploads/").mock(
        return_value=httpx.Response(202, headers={"Location": upload_url})
    )
    return mock_router.put(url__startswith=upload_url).mock(
        return_value=httpx.Response(status)
    )


@pytest.fixture
def monkeypatched_registry(monkeypatch):
    """Point both `DEFAULT_REGISTRY_URL` bindings at `MOCK_REGISTRY`.

    The dual-patch is mandatory: `auth.py` did `from .constants import
    DEFAULT_REGISTRY_URL` at import time, which copied the value into its
    own module namespace. Patching only `constants.DEFAULT_REGISTRY_URL`
    leaves `auth.DEFAULT_REGISTRY_URL` pointing at the production registry
    and the token-service GET escapes the respx mock — Task 2.1 lost an
    hour discovering this; encapsulating it here means Phase 6 tests just
    take the fixture and move on.

    Returns the mock registry URL so tests that need to build URLs
    themselves can read it from the fixture (rather than re-importing
    `MOCK_REGISTRY` separately).
    """
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)
    return MOCK_REGISTRY
