"""Smoke tests for the Phase 2.4 respx fixture module.

Each test exercises one fixture end-to-end against real production code
(`get_oci_bearer_token`, `fetch_manifest`) so the fixtures are themselves
verified — Phase 6 implementers reading this file get a working example
of how to compose the helpers without having to reverse-engineer them.

Why this lives next to `respx_fixtures.py` rather than inside it:
pytest's collection treats `tests/respx_fixtures.py` as a non-test module
(no `test_` prefix), so `@respx.mock` decorators there would never run.
Splitting the smoke tests out keeps the fixture module importable as a
pure utility (Phase 6 tests will do `from tests.respx_fixtures import ...`)
while still giving us live verification here.
"""
from __future__ import annotations

import httpx
import pytest
import respx

from tests.respx_fixtures import (
    MOCK_REGISTRY,
    blob_head_route,
    blob_put_route,
    manifest_get_route,
    manifest_put_route,
    token_route,
)


@respx.mock
def test_token_route_fixture_returns_mock_jwt(monkeypatched_registry):
    """`token_route` wires up a working `/service/token` mock end-to-end.

    Goes through `get_oci_bearer_token` (the real production code path)
    rather than asserting against the route directly — that way a future
    change to how `auth.py` builds the URL or sets headers would surface
    here instead of silently passing the fixture's mock while breaking
    production.
    """
    from hippius_hub.auth import get_oci_bearer_token

    route = token_route(respx.mock, scope="repository:foo/bar:pull")

    token = get_oci_bearer_token("foo/bar", token=False, use_cache=False)

    assert token == "mock-jwt-token"
    assert route.called, "token_route was registered but never invoked"


@respx.mock
def test_manifest_get_route_returns_digest(monkeypatched_registry):
    """`manifest_get_route` returns a parsed manifest plus Docker-Content-Digest.

    Drives `_oci.fetch_manifest` so we verify the route matches the URL
    template the production code actually emits — drift between the
    fixture's URL and `manifest_url()` would break every Phase 6 test
    silently, so catching it here is cheap insurance.
    """
    from hippius_hub._oci import fetch_manifest

    expected_digest = "sha256:" + "f" * 64
    manifest_get_route(respx.mock, "owner/repo", "main", digest=expected_digest)

    result = fetch_manifest(MOCK_REGISTRY, "owner/repo", "main", "fake-token")

    assert result is not None
    assert result.digest == expected_digest
    assert result.manifest["schemaVersion"] == 2


@respx.mock
def test_manifest_get_route_missing_returns_none(monkeypatched_registry):
    """`missing=True` drives `fetch_manifest`'s `missing_ok` path to None.

    Phase 6 tests covering fresh-repo / 404 manifest paths will pass
    `missing=True`; verifying that the route returns the right 404 shape
    here means those tests can trust the fixture and focus on their own
    assertions.
    """
    from hippius_hub._oci import fetch_manifest

    manifest_get_route(respx.mock, "owner/repo", "main", missing=True)

    result = fetch_manifest(
        MOCK_REGISTRY, "owner/repo", "main", "fake-token", missing_ok=True
    )

    assert result is None


@respx.mock
def test_manifest_get_route_omit_digest_header(monkeypatched_registry):
    """`omit_digest_header=True` simulates registries that don't emit Docker-Content-Digest.

    OCI Distribution Spec §4.4.1 marks that header as RECOMMENDED, not
    REQUIRED; this fixture lets Phase 6 tests exercise the
    `_prev_digest_or_warn` warning path without setting up a custom route
    each time.
    """
    from hippius_hub._oci import fetch_manifest

    manifest_get_route(respx.mock, "owner/repo", "main", omit_digest_header=True)

    result = fetch_manifest(MOCK_REGISTRY, "owner/repo", "main", "fake-token")

    assert result is not None
    assert result.digest is None


@respx.mock
def test_manifest_put_route_412_is_assertable(monkeypatched_registry):
    """`status=412` wires the concurrency-conflict response without exercising the caller.

    We assert on the response directly rather than through `_put_manifest`
    because `_put_manifest`'s 412 handler raises before returning — this
    smoke test is about the fixture, not the caller; tests of the raise
    behavior live in `test_upload_if_match.py`.
    """
    route = manifest_put_route(respx.mock, "owner/repo", "main", status=412)

    resp = httpx.put(f"{MOCK_REGISTRY}/v2/owner/repo/manifests/main")

    assert resp.status_code == 412
    assert route.called


@respx.mock
def test_blob_head_route_exists_and_missing(monkeypatched_registry):
    """`blob_head_route` returns 200 for `exists=True` and 404 for `exists=False`.

    Two-in-one because the routes target different digests, so they don't
    collide. Phase 6 tests of `_ensure_blob_uploaded` will combine
    `exists=False` with `blob_put_route` to drive the actual upload path.
    """
    present_digest = "sha256:" + "1" * 64
    absent_digest = "sha256:" + "2" * 64
    blob_head_route(respx.mock, "owner/repo", present_digest, exists=True)
    blob_head_route(respx.mock, "owner/repo", absent_digest, exists=False)

    present = httpx.head(f"{MOCK_REGISTRY}/v2/owner/repo/blobs/{present_digest}")
    absent = httpx.head(f"{MOCK_REGISTRY}/v2/owner/repo/blobs/{absent_digest}")

    assert present.status_code == 200
    assert absent.status_code == 404


@respx.mock
def test_blob_put_route_stubs_two_step_upload(monkeypatched_registry):
    """`blob_put_route` stubs both the POST init and the PUT completion.

    Exercises the same two-call sequence `_ensure_blob_uploaded` performs
    when a blob is missing: POST to `/uploads/` returns 202 with a
    `Location`, then PUT to that location (with a `?digest=...` suffix)
    returns 201. We don't drive the real `_ensure_blob_uploaded` here
    because it shells out to the Rust `upload_blob_native` and that needs
    a real socket — Phase 6 will mock at a higher level for those tests.
    """
    put_route = blob_put_route(respx.mock, "owner/repo")

    init = httpx.post(f"{MOCK_REGISTRY}/v2/owner/repo/blobs/uploads/")
    assert init.status_code == 202
    location = init.headers["Location"]
    assert location.startswith(f"{MOCK_REGISTRY}/v2/owner/repo/blobs/uploads/")

    upload = httpx.put(f"{location}?digest=sha256:cafe")
    assert upload.status_code == 201
    assert put_route.called


def test_monkeypatched_registry_dual_patches_both_bindings(monkeypatched_registry):
    """The fixture must patch BOTH `constants` AND `auth` namespaces.

    Regression guard: if a future refactor changes `auth.py` to read
    `DEFAULT_REGISTRY_URL` through the `constants` module rather than via
    its own imported binding, only one assertion would matter — but until
    then, missing either patch lets the token-service GET escape the mock
    and silently hit the real registry (Task 2.1's pain point).
    """
    from hippius_hub import auth, constants

    assert constants.DEFAULT_REGISTRY_URL == MOCK_REGISTRY
    assert auth.DEFAULT_REGISTRY_URL == MOCK_REGISTRY
