"""Audit M5/H1: two writers racing on the same `repo:revision` PUT.

The previous version of this file was single-threaded — it submitted one
PUT against a static 412 mock and asserted the typed error was raised.
That's a duplicate of `test_412_raises_ConcurrentManifestUpdateError` in
`test_upload_if_match.py`; it doesn't reproduce the race the audit found.

This rewrite actually races two ThreadPoolExecutor workers through
`upload_file` against respx mocks that:

  - GET manifest: always returns the same prior digest (so both writers
    fetch identical optimistic baselines)
  - PUT manifest: returns 201 on the FIRST request seen, 412 on every
    subsequent request — emulating Harbor's If-Match enforcement under a
    real concurrent write

The behavioral pin: exactly one worker must succeed, exactly one must
raise ConcurrentManifestUpdateError. The pre-fix `upload_file` (which
sent no If-Match header) would have both workers succeed silently — the
second one overwrites the first with no diagnostic.
"""
from __future__ import annotations

import base64
import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
import respx

from hippius_hub.errors import ConcurrentManifestUpdateError


REGISTRY = "https://registry.hippius.test"
REPO_ID = "owner/repo"
REVISION = "main"
PREV_DIGEST = "sha256:" + "a" * 64


def _valid_jwt() -> str:
    def b64(x):
        return base64.urlsafe_b64encode(json.dumps(x).encode()).decode().rstrip("=")
    return f"{b64({'alg': 'none'})}.{b64({'exp': int(time.time()) + 3600})}.signature"


def _existing_manifest_body() -> dict:
    return {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:" + hashlib.sha256(b"{}").hexdigest(),
            "size": 2,
        },
        "layers": [],
    }


@pytest.fixture(autouse=True)
def _point_registry_at_mock(monkeypatch):
    """Dual-binding patch — see test_upload_if_match.py for rationale."""
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_REGISTRY_URL", REGISTRY)
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_REGISTRY_URL", REGISTRY)


@respx.mock
def test_two_threaded_writers_one_wins_one_412s(tmp_path):
    """Two parallel `upload_file` invocations to the same revision must
    resolve to exactly one success and one ConcurrentManifestUpdateError.

    The PUT mock uses a side_effect closure so the FIRST request seen by
    respx receives 201; every subsequent request gets 412. This emulates
    Harbor's If-Match enforcement: the first writer's PUT advances the
    revision, the second writer's PUT — carrying the now-stale digest —
    is rejected by the registry.
    """
    payload_a = tmp_path / "a.txt"
    payload_b = tmp_path / "b.txt"
    payload_a.write_bytes(b"alpha\n")
    payload_b.write_bytes(b"bravo\n")

    sha_a = hashlib.sha256(b"alpha\n").hexdigest()
    sha_b = hashlib.sha256(b"bravo\n").hexdigest()

    # Token endpoint + blob HEADs for both payloads + the empty-config blob.
    respx.mock.get(url__startswith=f"{REGISTRY}/service/token").mock(
        return_value=httpx.Response(200, json={"token": _valid_jwt()})
    )
    empty_digest = "sha256:" + hashlib.sha256(b"{}").hexdigest()
    for digest in (f"sha256:{sha_a}", f"sha256:{sha_b}", empty_digest):
        respx.mock.head(f"{REGISTRY}/v2/{REPO_ID}/blobs/{digest}").mock(
            return_value=httpx.Response(200)
        )

    respx.mock.get(f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}").mock(
        return_value=httpx.Response(
            200,
            json=_existing_manifest_body(),
            headers={"Docker-Content-Digest": PREV_DIGEST},
        )
    )

    put_counter = {"count": 0}
    put_lock = threading.Lock()

    def put_side_effect(request):
        # The lock serializes the 201/412 decision but does NOT serialize
        # the upload_file calls themselves — those still execute on two
        # threads in parallel. The lock only guards the cursor that picks
        # which response this PUT gets, which is what real Harbor does at
        # the registry-storage layer (lock around the index file).
        with put_lock:
            put_counter["count"] += 1
            attempt = put_counter["count"]
        if attempt == 1:
            return httpx.Response(
                201,
                headers={"Docker-Content-Digest": "sha256:" + "b" * 64},
            )
        return httpx.Response(412)

    respx.mock.put(f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}").mock(
        side_effect=put_side_effect
    )

    def upload(path: Path, path_in_repo: str):
        # Imported inside the worker so threadpool boot doesn't slow the
        # respx-mock router setup.
        from hippius_hub.file_upload import upload_file
        return upload_file(
            path_or_fileobj=str(path),
            path_in_repo=path_in_repo,
            repo_id=REPO_ID,
            token="literal-token",
            revision=REVISION,
        )

    with ThreadPoolExecutor(max_workers=2) as ex:
        f_a = ex.submit(upload, payload_a, "a.txt")
        f_b = ex.submit(upload, payload_b, "b.txt")
        results = [f_a, f_b]

    successes = []
    failures = []
    for f in results:
        try:
            successes.append(f.result())
        except ConcurrentManifestUpdateError as e:
            failures.append(e)

    assert len(successes) == 1, (
        f"expected exactly one success, got {len(successes)}: {successes!r}"
    )
    assert len(failures) == 1, (
        f"expected exactly one ConcurrentManifestUpdateError, got "
        f"{len(failures)}: {failures!r}"
    )
    # PUT was called twice — once succeeding, once failing with 412.
    assert put_counter["count"] == 2, (
        f"expected 2 PUTs, got {put_counter['count']}"
    )


@respx.mock
def test_serial_412_still_raises_typed_error(tmp_path):
    """Sanity coverage: the single-threaded 412 case (the previous shape
    of this file) is preserved as a regression pin. The audit-tag note
    here is for grep-discoverability — the same scenario is also covered
    by test_412_raises_ConcurrentManifestUpdateError in
    test_upload_if_match.py, but having one test named `test_concurrent_*`
    that fails fast on the regression is helpful when triaging."""
    payload = tmp_path / "single.txt"
    payload.write_bytes(b"solo\n")
    sha = hashlib.sha256(b"solo\n").hexdigest()

    respx.mock.get(url__startswith=f"{REGISTRY}/service/token").mock(
        return_value=httpx.Response(200, json={"token": _valid_jwt()})
    )
    empty_digest = "sha256:" + hashlib.sha256(b"{}").hexdigest()
    for digest in (f"sha256:{sha}", empty_digest):
        respx.mock.head(f"{REGISTRY}/v2/{REPO_ID}/blobs/{digest}").mock(
            return_value=httpx.Response(200)
        )
    respx.mock.get(f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}").mock(
        return_value=httpx.Response(
            200,
            json=_existing_manifest_body(),
            headers={"Docker-Content-Digest": PREV_DIGEST},
        )
    )
    respx.mock.put(f"{REGISTRY}/v2/{REPO_ID}/manifests/{REVISION}").mock(
        return_value=httpx.Response(412)
    )

    from hippius_hub.file_upload import upload_file
    with pytest.raises(ConcurrentManifestUpdateError):
        upload_file(
            path_or_fileobj=str(payload),
            path_in_repo="single.txt",
            repo_id=REPO_ID,
            token="literal-token",
            revision=REVISION,
        )
