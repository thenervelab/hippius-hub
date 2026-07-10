"""Regression tests for OCI If-Match on manifest PUT (audit H1, M5).

The TOCTOU window between fetch_manifest and the PUT that follows it lets
two concurrent uploads to the same repo:revision silently overwrite each
other. The fix is to send `If-Match: <previous-manifest-digest>` on the PUT
so the registry returns 412 Precondition Failed when a concurrent writer
has already advanced the revision.

These tests mock the OCI registry with respx — no real network is hit. They
cover four cases:

  1. When the manifest existed (fetch returned a digest), the next PUT MUST
     carry `If-Match: <that-digest>`.
  2. When the registry returns 412 to the PUT, we MUST raise the typed
     `ConcurrentManifestUpdateError` (not a generic HTTPStatusError).
  3. When there is no prior manifest (fresh repo, 404 fetch), the PUT MUST
     NOT carry an If-Match header — there's nothing to be optimistic about.
  4. When the manifest fetch succeeded but the registry omitted the
     `Docker-Content-Digest` response header (RECOMMENDED-but-not-REQUIRED
     per OCI Distribution Spec §4.4.1), the PUT MUST proceed without
     If-Match AND the call MUST emit a UserWarning so operators see that
     this revision was written without optimistic-concurrency protection.
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

from hippius_hub.errors import ConcurrentManifestUpdateError, HfHubHTTPError


def _valid_jwt() -> str:
    """Build a 3-segment JWT with a far-future exp.

    The token-service stub used to return `"fake-oci-jwt"`, which has no
    dots — every call into `_jwt_expiration` emitted the
    `"JWT does not have 3 segments"` UserWarning. Tests should not leak
    warnings they don't assert on, so we synthesize a structurally-valid
    token here. The signature segment is a placeholder: `_jwt_expiration`
    inspects only the payload (middle segment).
    """
    def b64(x):
        return base64.urlsafe_b64encode(json.dumps(x).encode()).decode().rstrip("=")
    return f"{b64({'alg': 'none'})}.{b64({'exp': int(time.time()) + 3600})}.signature"


REGISTRY = "https://registry.hippius.test"
REPO_ID = "owner/repo"
REVISION = "main"
PREV_DIGEST = "sha256:" + "a" * 64


def _write_payload(tmp_path: Path, name: str = "hello.txt") -> tuple[Path, str, int]:
    """Drop a small file in tmp_path and return (path, sha256_hex, size).

    Returning the precomputed digest lets the respx blob-HEAD route be
    configured before the upload runs.
    """
    data = b"hello world\n"
    target = tmp_path / name
    target.write_bytes(data)
    return target, hashlib.sha256(data).hexdigest(), len(data)


def _stub_auth_and_blob(mock: respx.MockRouter, blob_digest: str) -> None:
    """Wire the token-service + blob-HEAD routes that every upload path needs.

    Blob HEAD returns 200 so the test does NOT exercise the native blob
    uploader (which would require a real socket). The manifest path is what
    matters for If-Match coverage.
    """
    mock.get(
        url__startswith=f"{REGISTRY}/service/token",
    ).mock(return_value=httpx.Response(200, json={"token": _valid_jwt()}))
    mock.head(
        f"{REGISTRY}/v2/{REPO_ID}/blobs/sha256:{blob_digest}"
    ).mock(return_value=httpx.Response(200))
    # The empty-object config blob is always pushed; allow the HEAD to say
    # "already there" so we don't need to mock the POST/PUT for it either.
    empty_digest = "sha256:" + hashlib.sha256(b"{}").hexdigest()
    mock.head(
        f"{REGISTRY}/v2/{REPO_ID}/blobs/{empty_digest}"
    ).mock(return_value=httpx.Response(200))


@pytest.fixture(autouse=True)
def _point_registry_at_mock(monkeypatch):
    """Force every registry lookup to hit the respx mock URL.

    Both bindings have to be patched: `constants.DEFAULT_REGISTRY_URL` is
    what `resolve_registry()` reads, but `auth.py` did `from .constants
    import DEFAULT_REGISTRY_URL` at import time and now holds its own
    name binding — patching only the constants module leaves auth.py
    pointed at the real registry and the token-service GET escapes the
    mock. Patching both keeps the test offline.
    """
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_REGISTRY_URL", REGISTRY)
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_REGISTRY_URL", REGISTRY)


@respx.mock
def test_put_manifest_sends_if_match_when_previous_digest_known(tmp_path):
    """When fetch_manifest returned a digest, the next PUT must send If-Match.

    Race-window closure: the second uploader's PUT now carries the digest
    the first uploader saw. If a concurrent writer advanced the manifest in
    between, the server rejects with 412 (covered by the next test).
    """
    from hippius_hub.file_upload import upload_file

    payload, sha_hex, _size = _write_payload(tmp_path)
    _stub_auth_and_blob(respx.mock, sha_hex)

    existing_manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.empty.v1+json",
                   "digest": "sha256:" + hashlib.sha256(b"{}").hexdigest(),
                   "size": 2},
        "layers": [],
    }
    respx.mock.get(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(
        200,
        json=existing_manifest,
        headers={"Docker-Content-Digest": PREV_DIGEST},
    ))
    put_route = respx.mock.put(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(
        201,
        headers={"Docker-Content-Digest": "sha256:" + "b" * 64},
    ))

    upload_file(
        path_or_fileobj=str(payload),
        path_in_repo="hello.txt",
        repo_id=REPO_ID,
        token="literal-token-value",
        revision=REVISION,
    )

    assert put_route.called, "manifest PUT was never invoked"
    sent_headers = put_route.calls.last.request.headers
    assert sent_headers.get("If-Match") == PREV_DIGEST, (
        f"PUT must carry If-Match={PREV_DIGEST!r}, "
        f"got If-Match={sent_headers.get('If-Match')!r}"
    )


@respx.mock
def test_412_raises_ConcurrentManifestUpdateError(tmp_path):
    """A 412 from the registry must surface as the typed exception.

    Catching `HfHubHTTPError` must also catch it, so callers that already
    handle the HF-style hierarchy don't need a special case.
    """
    from hippius_hub.file_upload import upload_file

    payload, sha_hex, _size = _write_payload(tmp_path)
    _stub_auth_and_blob(respx.mock, sha_hex)

    respx.mock.get(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(
        200,
        json={"schemaVersion": 2,
              "config": {"digest": "sha256:" + hashlib.sha256(b"{}").hexdigest(),
                         "size": 2},
              "layers": []},
        headers={"Docker-Content-Digest": PREV_DIGEST},
    ))
    respx.mock.put(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(412))

    with pytest.raises(ConcurrentManifestUpdateError) as exc_info:
        upload_file(
            path_or_fileobj=str(payload),
            path_in_repo="hello.txt",
            repo_id=REPO_ID,
            token="literal-token-value",
            revision=REVISION,
        )
    # HF parity: callers writing `except HfHubHTTPError` should still catch us.
    assert isinstance(exc_info.value, HfHubHTTPError)
    assert REPO_ID in str(exc_info.value)
    assert REVISION in str(exc_info.value)


@respx.mock
def test_no_if_match_when_no_prior_manifest(tmp_path):
    """Fresh repo: manifest GET returns 404, so the PUT carries no If-Match.

    Sending If-Match with `*` or an empty value here would either be wrong
    semantics (no prior digest exists) or accidentally turn into a "create
    only" precondition. The right behavior is to omit the header.
    """
    from hippius_hub.file_upload import upload_file

    payload, sha_hex, _size = _write_payload(tmp_path)
    _stub_auth_and_blob(respx.mock, sha_hex)

    respx.mock.get(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(404))
    put_route = respx.mock.put(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(
        201,
        headers={"Docker-Content-Digest": "sha256:" + "c" * 64},
    ))

    upload_file(
        path_or_fileobj=str(payload),
        path_in_repo="hello.txt",
        repo_id=REPO_ID,
        token="literal-token-value",
        revision=REVISION,
    )

    assert put_route.called, "manifest PUT was never invoked"
    sent_headers = put_route.calls.last.request.headers
    assert "If-Match" not in sent_headers, (
        f"fresh-repo PUT must not carry If-Match, "
        f"got If-Match={sent_headers.get('If-Match')!r}"
    )


@respx.mock
def test_warns_when_prior_manifest_lacks_docker_content_digest(tmp_path):
    """Registry honored the fetch but omitted Docker-Content-Digest.

    OCI Distribution Spec §4.4.1 marks Docker-Content-Digest as RECOMMENDED
    but not REQUIRED on manifest responses. Harbor sends it today, but a
    proxy that strips the header (or a future registry change) would put
    this code path into production silently. We can't synthesize the digest
    — the body has been re-serialized into a dict — so the PUT MUST proceed
    without If-Match, but the operator MUST see a warning naming the
    repo:revision so the unprotected write is grep-able.
    """
    from hippius_hub.file_upload import upload_file

    payload, sha_hex, _size = _write_payload(tmp_path)
    _stub_auth_and_blob(respx.mock, sha_hex)

    existing_manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.empty.v1+json",
                   "digest": "sha256:" + hashlib.sha256(b"{}").hexdigest(),
                   "size": 2},
        "layers": [],
    }
    # Deliberately omit Docker-Content-Digest: this is the regression target.
    respx.mock.get(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(200, json=existing_manifest))
    put_route = respx.mock.put(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(
        201,
        headers={"Docker-Content-Digest": "sha256:" + "d" * 64},
    ))

    with pytest.warns(UserWarning, match="Docker-Content-Digest"):
        upload_file(
            path_or_fileobj=str(payload),
            path_in_repo="hello.txt",
            repo_id=REPO_ID,
            token="literal-token-value",
            revision=REVISION,
        )

    assert put_route.called, "manifest PUT was never invoked"
    sent_headers = put_route.calls.last.request.headers
    assert "If-Match" not in sent_headers, (
        f"PUT must not carry If-Match when prior digest is unknown, "
        f"got If-Match={sent_headers.get('If-Match')!r}"
    )


# --- Manifest-PUT retry on the Harbor blob-commit visibility race -------------
#
# Harbor accepts a blob (201) seconds before it is visible to manifest
# validation (a measured ~3.4s "Move" window). A fast client PUTs the manifest
# inside that window and gets a transient 400 MANIFEST_BLOB_UNKNOWN. Since the
# manifest is deterministic and the write is idempotent, _put_manifest retries
# transient 4xx/5xx with backoff. These tests pin that behavior offline.

_BLOB_UNKNOWN_BODY = {
    "errors": [{"code": "MANIFEST_BLOB_UNKNOWN", "message": "blob unknown to registry",
                "detail": "sha256:" + "e" * 64}]
}


@respx.mock
def test_manifest_put_retries_transient_400_then_succeeds(tmp_path, monkeypatch):
    """A 400 MANIFEST_BLOB_UNKNOWN then a 201: the upload retries and succeeds.

    This is the exact staging-CI failure — a just-committed pack not yet visible
    to manifest validation. The second PUT (after backoff) lands once Harbor has
    finished the blob commit.
    """
    from hippius_hub.file_upload import upload_file

    sleeps: list[float] = []
    monkeypatch.setattr("hippius_hub.file_upload.time.sleep", lambda s: sleeps.append(s))

    payload, sha_hex, _size = _write_payload(tmp_path)
    _stub_auth_and_blob(respx.mock, sha_hex)
    respx.mock.get(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(404))
    put_route = respx.mock.put(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(side_effect=[
        httpx.Response(400, json=_BLOB_UNKNOWN_BODY),
        httpx.Response(201, headers={"Docker-Content-Digest": "sha256:" + "f" * 64}),
    ])

    upload_file(
        path_or_fileobj=str(payload),
        path_in_repo="hello.txt",
        repo_id=REPO_ID,
        token="literal-token-value",
        revision=REVISION,
    )

    assert put_route.call_count == 2, "PUT must be retried exactly once after the 400"
    assert sleeps == [0.5], "one bounded backoff sleep between the two attempts"


@respx.mock
def test_manifest_put_exhausts_retries_and_surfaces_error_body(tmp_path, monkeypatch):
    """A persistent 400 exhausts the bounded retries and raises with the OCI body.

    `raise_for_status` used to hide Harbor's error code; the raised message must
    now carry MANIFEST_BLOB_UNKNOWN (and repo:revision) so a real, non-transient
    manifest rejection is diagnosable instead of an opaque `400 Bad Request`.
    """
    from hippius_hub.file_upload import MANIFEST_PUT_MAX_RETRIES, upload_file

    monkeypatch.setattr("hippius_hub.file_upload.time.sleep", lambda s: None)

    payload, sha_hex, _size = _write_payload(tmp_path)
    _stub_auth_and_blob(respx.mock, sha_hex)
    respx.mock.get(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(404))
    put_route = respx.mock.put(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(400, json=_BLOB_UNKNOWN_BODY))

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        upload_file(
            path_or_fileobj=str(payload),
            path_in_repo="hello.txt",
            repo_id=REPO_ID,
            token="literal-token-value",
            revision=REVISION,
        )

    assert put_route.call_count == MANIFEST_PUT_MAX_RETRIES + 1
    msg = str(exc_info.value)
    assert "MANIFEST_BLOB_UNKNOWN" in msg, "Harbor's error body must be surfaced"
    assert REPO_ID in msg and REVISION in msg


@respx.mock
def test_manifest_put_does_not_retry_non_transient_403(tmp_path, monkeypatch):
    """A 403 is a permanent rejection (auth/quota), not the commit race: fail fast.

    Retrying a genuinely-rejected PUT would just burn the backoff budget before
    surfacing the same error, so only the transient status set is retried.
    """
    from hippius_hub.file_upload import upload_file

    sleeps: list[float] = []
    monkeypatch.setattr("hippius_hub.file_upload.time.sleep", lambda s: sleeps.append(s))

    payload, sha_hex, _size = _write_payload(tmp_path)
    _stub_auth_and_blob(respx.mock, sha_hex)
    respx.mock.get(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(404))
    put_route = respx.mock.put(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(403, text="denied"))

    with pytest.raises(httpx.HTTPStatusError):
        upload_file(
            path_or_fileobj=str(payload),
            path_in_repo="hello.txt",
            repo_id=REPO_ID,
            token="literal-token-value",
            revision=REVISION,
        )

    assert put_route.call_count == 1, "non-transient status must not be retried"
    assert sleeps == [], "no backoff for a permanent rejection"
