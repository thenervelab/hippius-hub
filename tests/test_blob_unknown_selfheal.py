"""Client self-heal when the registry loses a blob it just accepted (#3).

Harbor's manifest PUT validates every referenced blob against its own DB. A pack
whose upload returned 2xx but whose commit never durably landed — a registry-side
GC reap of an untagged blob, or a "Move" lost under mount pressure — makes the
manifest PUT fail `MANIFEST_BLOB_UNKNOWN` forever: `_put_manifest`'s own retry
budget only rides out the *transient* commit→visibility lag, it can't conjure a
blob that is genuinely gone.

`upload_file` now treats a MANIFEST_BLOB_UNKNOWN that outlives that budget as a
durable loss and RE-RUNS the whole upload (packs always re-upload; the config
cache is evicted at the raise site), bounded by `blob_reupload_max_retries()` so a
registry that keeps dropping the blob surfaces as the infra fault it is rather
than looping forever. These tests pin: re-upload-then-succeed, the bound, that a
non-BLOB_UNKNOWN failure is NOT re-uploaded, and the digest extraction.
"""
from __future__ import annotations

import io
from types import SimpleNamespace

import httpx
import pytest

from hippius_hub import auth, file_upload
from hippius_hub.errors import HfHubHTTPError, ManifestBlobUnknownError
from hippius_hub.file_upload import blob_reupload_max_retries


def _blob_unknown_response(digest: str) -> httpx.Response:
    """A Harbor MANIFEST_BLOB_UNKNOWN 400 naming `digest`."""
    return httpx.Response(
        400,
        request=httpx.Request("PUT", "http://x/v2/acme/model/manifests/main"),
        json={"errors": [{"code": "MANIFEST_BLOB_UNKNOWN",
                          "message": "blob unknown to registry", "detail": digest}]},
    )


def _stub_upload_ok(monkeypatch) -> list:
    """Wire a minimal, offline upload_file: token, no prior manifest, config
    present, a commit builder. Returns the list that records each layer-upload
    (re-run) so a test can count how many times the full upload re-ran."""
    auth.clear_oci_token_cache()
    monkeypatch.setattr(auth, "get_oci_bearer_token", lambda *a, **k: "tok")
    monkeypatch.setattr(file_upload, "fetch_manifest", lambda *a, **k: None)
    monkeypatch.setattr(
        file_upload, "_ensure_config_blob_uploaded", lambda *a, **k: ("sha256:" + "c" * 64, 2)
    )
    monkeypatch.setattr(file_upload, "_build_commit_info", lambda *a, **k: "commit-ok")

    layer_uploads: list[int] = []
    monkeypatch.setattr(
        file_upload, "_upload_file_layers", lambda *a, **k: layer_uploads.append(1) or []
    )
    return layer_uploads


DIGEST = "sha256:" + "e" * 64


def test_reuploads_and_succeeds_when_a_blob_was_lost(monkeypatch):
    # First full attempt: the manifest names a blob the registry lost → re-upload.
    # Second attempt: the re-pushed blob is now present → commit lands.
    layer_uploads = _stub_upload_ok(monkeypatch)

    put_calls: list[int] = []

    def fake_put_manifest(*a, **k):
        put_calls.append(1)
        if len(put_calls) == 1:
            raise ManifestBlobUnknownError(
                "manifest PUT for acme/model:main failed with 400 after 13 attempt(s): "
                "MANIFEST_BLOB_UNKNOWN",
                response=_blob_unknown_response(DIGEST),
                missing_digests=(DIGEST,),
            )
        return SimpleNamespace(headers={})

    monkeypatch.setattr(file_upload, "_put_manifest", fake_put_manifest)

    result = file_upload.upload_file(
        path_or_fileobj=io.BytesIO(b"real content"),
        path_in_repo="model.bin",
        repo_id="acme/model",
        repo_type="model",
    )

    assert result == "commit-ok"
    assert len(layer_uploads) == 2, "a lost blob must trigger exactly one full re-upload"
    assert len(put_calls) == 2


def test_bounded_reuploads_then_surfaces_the_error(monkeypatch):
    # The registry keeps dropping the blob (an infra fault): re-upload the bounded
    # number of times, then surface the typed error rather than loop forever.
    layer_uploads = _stub_upload_ok(monkeypatch)

    def always_blob_unknown(*a, **k):
        raise ManifestBlobUnknownError(
            "manifest PUT for acme/model:main failed with 400 after 13 attempt(s): "
            "MANIFEST_BLOB_UNKNOWN",
            response=_blob_unknown_response(DIGEST),
            missing_digests=(DIGEST,),
        )

    monkeypatch.setattr(file_upload, "_put_manifest", always_blob_unknown)

    with pytest.raises(ManifestBlobUnknownError) as exc_info:
        file_upload.upload_file(
            path_or_fileobj=io.BytesIO(b"real content"),
            path_in_repo="model.bin",
            repo_id="acme/model",
            repo_type="model",
        )

    assert len(layer_uploads) == blob_reupload_max_retries() + 1, (
        "the upload must re-run exactly blob_reupload_max_retries() + 1 times before giving up"
    )
    assert isinstance(exc_info.value, HfHubHTTPError), "callers catching HfHubHTTPError must still catch it"
    assert exc_info.value.missing_digests == (DIGEST,)


def test_a_non_blob_unknown_failure_is_not_reuploaded(monkeypatch):
    # A MANIFEST_INVALID (or any non-BLOB_UNKNOWN error) is a permanent client
    # fault — re-uploading identical bytes would just fail again, so it must
    # propagate on the first attempt with no re-upload.
    layer_uploads = _stub_upload_ok(monkeypatch)

    def malformed(*a, **k):
        raise httpx.HTTPStatusError(
            "manifest PUT failed with 400: MANIFEST_INVALID",
            request=httpx.Request("PUT", "http://x"),
            response=httpx.Response(400, text='{"errors":[{"code":"MANIFEST_INVALID"}]}'),
        )

    monkeypatch.setattr(file_upload, "_put_manifest", malformed)

    with pytest.raises(httpx.HTTPStatusError):
        file_upload.upload_file(
            path_or_fileobj=io.BytesIO(b"real content"),
            path_in_repo="model.bin",
            repo_id="acme/model",
            repo_type="model",
        )

    assert len(layer_uploads) == 1, "a non-BLOB_UNKNOWN error must not trigger a re-upload"


def test_blob_unknown_digests_extracts_named_blobs():
    resp = _blob_unknown_response(DIGEST)
    assert file_upload._blob_unknown_digests(resp) == (DIGEST,)


def test_blob_unknown_digests_dedupes_and_preserves_order():
    a, b = "sha256:" + "a" * 64, "sha256:" + "b" * 64
    resp = httpx.Response(400, text=f'{{"errors":[{{"detail":"{a}"}},{{"detail":"{b}"}},{{"detail":"{a}"}}]}}')
    assert file_upload._blob_unknown_digests(resp) == (a, b)


def test_blob_unknown_digests_empty_body_is_empty_tuple():
    assert file_upload._blob_unknown_digests(httpx.Response(400, text="")) == ()
