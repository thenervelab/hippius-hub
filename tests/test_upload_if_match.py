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

from hippius_hub.errors import ConcurrentManifestUpdateError, HfHubHTTPError, ManifestBlobUnknownError


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


# --- Manifest-PUT retry on transient conditions -------------------------------
#
# Harbor accepts a blob (201) seconds before it is visible to manifest
# validation (a measured ~3.4s "Move" window). A fast client PUTs the manifest
# inside that window and gets a 400 MANIFEST_BLOB_UNKNOWN. Since the manifest is
# deterministic and the write is idempotent, _put_manifest retries — with the
# same transient set the Rust uploader retries (connection errors + 408/429/5xx)
# plus that one Harbor-specific BLOB_UNKNOWN 400 — and fails fast on any other
# status. These tests pin that classification offline.

_BLOB_UNKNOWN_BODY = {
    "errors": [{"code": "MANIFEST_BLOB_UNKNOWN", "message": "blob unknown to registry",
                "detail": "sha256:" + "e" * 64}]
}
_MANIFEST_INVALID_BODY = {
    "errors": [{"code": "MANIFEST_INVALID", "message": "manifest invalid"}]
}


def _no_backoff(monkeypatch) -> list:
    """Silence real backoff and pin jitter deterministic; return the sleep log.

    random() -> 1.0 makes the jittered delay equal the raw backoff, so a sleep
    assertion reads the intended schedule (0.5, 1, 2, ...) rather than a random
    fraction of it.
    """
    sleeps: list[float] = []
    monkeypatch.setattr("hippius_hub.file_upload.time.sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr("hippius_hub.file_upload.random.random", lambda: 1.0)
    return sleeps


@respx.mock
def test_manifest_put_retries_blob_unknown_400_then_succeeds(tmp_path, monkeypatch):
    """A 400 MANIFEST_BLOB_UNKNOWN then a 201: the upload retries and succeeds.

    This is the exact staging-CI failure — a just-committed pack not yet visible
    to manifest validation. The second PUT (after backoff) lands once Harbor has
    finished the blob commit.
    """
    from hippius_hub.file_upload import upload_file

    sleeps = _no_backoff(monkeypatch)
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
def test_manifest_put_rides_out_a_lag_longer_than_the_old_budget(tmp_path, monkeypatch):
    """The commit-visibility lag has grown under sustained JuiceFS backpressure past
    the original ~13s / 5-retry window (a fast CI runner outruns it). The widened
    budget must ride out a run of BLOB_UNKNOWN 400s LONGER than the old 6-attempt
    window and still land the manifest — this is the pre-widening staging failure.
    """
    from hippius_hub.file_upload import MANIFEST_PUT_MAX_RETRIES, upload_file

    _no_backoff(monkeypatch)
    payload, sha_hex, _size = _write_payload(tmp_path)
    _stub_auth_and_blob(respx.mock, sha_hex)
    respx.mock.get(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(404))
    # 8 consecutive BLOB_UNKNOWN 400s — beyond the OLD 6-attempt budget — then a 201.
    lag = 8
    assert MANIFEST_PUT_MAX_RETRIES + 1 > lag, "the widened budget must exceed the simulated lag"
    put_route = respx.mock.put(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(side_effect=(
        [httpx.Response(400, json=_BLOB_UNKNOWN_BODY)] * lag
        + [httpx.Response(201, headers={"Docker-Content-Digest": "sha256:" + "b" * 64})]
    ))

    upload_file(
        path_or_fileobj=str(payload),
        path_in_repo="hello.txt",
        repo_id=REPO_ID,
        token="literal-token-value",
        revision=REVISION,
    )

    assert put_route.call_count == lag + 1, (
        "the manifest PUT must keep retrying past the old budget until the blob is visible"
    )


@respx.mock
def test_manifest_put_retries_5xx_then_succeeds(tmp_path, monkeypatch):
    """A 503 (registry redeploy/overload) is transient and retried, like in Rust."""
    from hippius_hub.file_upload import upload_file

    sleeps = _no_backoff(monkeypatch)
    payload, sha_hex, _size = _write_payload(tmp_path)
    _stub_auth_and_blob(respx.mock, sha_hex)
    respx.mock.get(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(404))
    put_route = respx.mock.put(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(side_effect=[
        httpx.Response(503, text="service unavailable"),
        httpx.Response(201, headers={"Docker-Content-Digest": "sha256:" + "a" * 64}),
    ])

    upload_file(
        path_or_fileobj=str(payload),
        path_in_repo="hello.txt",
        repo_id=REPO_ID,
        token="literal-token-value",
        revision=REVISION,
    )

    assert put_route.call_count == 2
    assert sleeps == [0.5]


@respx.mock
def test_manifest_put_retries_transport_error_then_succeeds(tmp_path, monkeypatch):
    """A connection reset mid-PUT (an exception, not a status) is retried too.

    Rust's is_retryable covers timeouts/connection errors; the Python loop must
    match — a Harbor redeploy that drops the socket is at least as transient as a
    503, and used to fail the whole upload on the first blip.
    """
    from hippius_hub.file_upload import upload_file

    sleeps = _no_backoff(monkeypatch)
    payload, sha_hex, _size = _write_payload(tmp_path)
    _stub_auth_and_blob(respx.mock, sha_hex)
    respx.mock.get(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(404))
    put_route = respx.mock.put(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(side_effect=[
        httpx.ConnectError("connection reset by peer"),
        httpx.Response(201, headers={"Docker-Content-Digest": "sha256:" + "b" * 64}),
    ])

    upload_file(
        path_or_fileobj=str(payload),
        path_in_repo="hello.txt",
        repo_id=REPO_ID,
        token="literal-token-value",
        revision=REVISION,
    )

    assert put_route.call_count == 2, "the transport error must be retried, not fatal"
    assert sleeps == [0.5]


@respx.mock
def test_manifest_put_does_not_retry_malformed_400(tmp_path, monkeypatch):
    """A 400 that is NOT the blob-commit race (MANIFEST_INVALID) fails fast.

    This is the load-bearing narrowing: a genuinely-bad manifest must surface
    immediately with its real code, not burn 6 PUTs / ~15s mislabelled as a
    transient registry race.
    """
    from hippius_hub.file_upload import upload_file

    sleeps = _no_backoff(monkeypatch)
    payload, sha_hex, _size = _write_payload(tmp_path)
    _stub_auth_and_blob(respx.mock, sha_hex)
    respx.mock.get(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(404))
    put_route = respx.mock.put(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(400, json=_MANIFEST_INVALID_BODY))

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        upload_file(
            path_or_fileobj=str(payload),
            path_in_repo="hello.txt",
            repo_id=REPO_ID,
            token="literal-token-value",
            revision=REVISION,
        )

    assert put_route.call_count == 1, "a non-race 400 must not be retried"
    assert sleeps == [], "no backoff for a permanent 400"
    assert "MANIFEST_INVALID" in str(exc_info.value), "the real error code must be surfaced"


@respx.mock
def test_manifest_put_exhausts_retries_and_reuploads_then_surfaces_error_body(tmp_path, monkeypatch):
    """A persistent BLOB_UNKNOWN 400 exhausts the per-PUT retries AND the bounded
    blob re-uploads, then raises the typed ManifestBlobUnknownError with the OCI body.

    A blob the registry keeps dropping can neither be waited out (the per-PUT budget)
    nor re-pushed away (the re-upload budget), so the client re-runs the whole upload
    BLOB_REUPLOAD_MAX_RETRIES + 1 times — each burning the full manifest-PUT budget —
    and then surfaces MANIFEST_BLOB_UNKNOWN as a TYPED error carrying repo:revision and
    the body, so callers can tell it apart from a permanent 400.
    """
    from hippius_hub.file_upload import (
        MANIFEST_PUT_MAX_RETRIES,
        blob_reupload_max_retries,
        upload_file,
    )

    _no_backoff(monkeypatch)
    payload, sha_hex, _size = _write_payload(tmp_path)
    _stub_auth_and_blob(respx.mock, sha_hex)
    respx.mock.get(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(404))
    put_route = respx.mock.put(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(400, json=_BLOB_UNKNOWN_BODY))

    with pytest.raises(ManifestBlobUnknownError) as exc_info:
        upload_file(
            path_or_fileobj=str(payload),
            path_in_repo="hello.txt",
            repo_id=REPO_ID,
            token="literal-token-value",
            revision=REVISION,
        )

    assert put_route.call_count == (MANIFEST_PUT_MAX_RETRIES + 1) * (blob_reupload_max_retries() + 1), (
        "every re-upload burns the full manifest-PUT budget before the next re-upload"
    )
    assert isinstance(exc_info.value, HfHubHTTPError), "must stay catchable as HfHubHTTPError"
    msg = str(exc_info.value)
    assert "MANIFEST_BLOB_UNKNOWN" in msg, "Harbor's error body must be surfaced"
    assert REPO_ID in msg and REVISION in msg


@respx.mock
def test_persistent_blob_unknown_reconfirms_the_config_blob_on_reupload(tmp_path, monkeypatch):
    """The config-lost self-heal: a persistent BLOB_UNKNOWN evicts the `{}`-config
    cache so the re-upload re-confirms it through the REAL `_ensure_config_blob_uploaded`
    instead of cache-skipping and failing identically.

    This pins the eviction in `_put_manifest` — the mechanism for the PR's headline
    scenario (a GC-reaped empty config). Remove the eviction and the re-run reuses the
    cached config, so the config HEAD is not re-issued and this assertion drops from 2
    to 1. Runs the real config helper (not a stub), so it actually exercises the link.
    """
    from hippius_hub.file_upload import (
        MANIFEST_PUT_MAX_RETRIES,
        clear_config_blob_cache,
        upload_file,
    )

    clear_config_blob_cache()
    _no_backoff(monkeypatch)
    payload, sha_hex, _size = _write_payload(tmp_path)

    empty_digest = "sha256:" + hashlib.sha256(b"{}").hexdigest()
    respx.mock.get(
        url__startswith=f"{REGISTRY}/service/token"
    ).mock(return_value=httpx.Response(200, json={"token": _valid_jwt()}))
    respx.mock.head(
        f"{REGISTRY}/v2/{REPO_ID}/blobs/sha256:{sha_hex}"
    ).mock(return_value=httpx.Response(200))
    config_head = respx.mock.head(
        f"{REGISTRY}/v2/{REPO_ID}/blobs/{empty_digest}"
    ).mock(return_value=httpx.Response(200))
    respx.mock.get(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(return_value=httpx.Response(404))
    # The whole first attempt 400s BLOB_UNKNOWN (exhausts the per-PUT budget → evicts the
    # config cache → re-upload); the re-upload's single manifest PUT then lands.
    put_route = respx.mock.put(
        f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}"
    ).mock(side_effect=(
        [httpx.Response(400, json=_BLOB_UNKNOWN_BODY)] * (MANIFEST_PUT_MAX_RETRIES + 1)
        + [httpx.Response(201, headers={"Docker-Content-Digest": "sha256:" + "e" * 64})]
    ))

    upload_file(
        path_or_fileobj=str(payload),
        path_in_repo="hello.txt",
        repo_id=REPO_ID,
        token="literal-token-value",
        revision=REVISION,
    )

    assert config_head.call_count == 2, (
        "the persistent BLOB_UNKNOWN must evict the {}-config cache so the re-upload "
        "re-confirms it via the real _ensure_config_blob_uploaded — without the eviction "
        "the config stays cached and this drops to 1"
    )
    assert put_route.call_count == MANIFEST_PUT_MAX_RETRIES + 2, (
        "the full per-PUT budget on the first attempt, then one landing PUT on the re-upload"
    )


@respx.mock
def test_manifest_put_does_not_retry_non_transient_403(tmp_path, monkeypatch):
    """A 403 is a permanent rejection (auth/quota), not the commit race: fail fast.

    Retrying a genuinely-rejected PUT would just burn the backoff budget before
    surfacing the same error, so only the transient set is retried.
    """
    from hippius_hub.file_upload import upload_file

    sleeps = _no_backoff(monkeypatch)
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
