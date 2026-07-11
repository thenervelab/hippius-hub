"""Audit M2: refresh the OCI bearer token on a 401 and retry the operation once.

A token minted up front can expire mid-operation on a long transfer (the per-op
token is threaded into a multi-GB native upload/download that outlives its TTL).
`auth.call_with_oci_token_refresh` runs the operation, and on a 401 clears the
token cache, re-mints a fresh token, and retries ONCE. A second 401 is a genuine
authorization failure and propagates.

The helper unit tests pin the predicate + the retry-once/refresh semantics; the
download wiring test proves `hf_hub_download` actually routes through the wrapper
so a native 401 is transparently recovered.
"""
from __future__ import annotations

import io
from types import SimpleNamespace

import httpx
import pytest
import respx

from hippius_hub import auth, file_download, file_upload
from hippius_hub.constants import LAYER_TITLE_KEY
from hippius_hub.file_download import hf_hub_download

from tests.respx_fixtures import MOCK_REGISTRY, token_route


# ---- predicate: _is_oci_auth_error ----


def test_is_oci_auth_error_matches_native_401_message():
    # The Rust extension surfaces CoreError::ServerError(401) as this message.
    assert auth._is_oci_auth_error(RuntimeError("server returned 401 (Unauthorized)"))
    assert not auth._is_oci_auth_error(RuntimeError("server returned 500 (boom)"))
    assert not auth._is_oci_auth_error(RuntimeError("some unrelated failure"))


def test_is_oci_auth_error_matches_httpx_response_401():
    # An httpx response error carries .response.status_code (raise_for_status path).
    err_401 = httpx.HTTPStatusError("401", request=httpx.Request("GET", "http://x"),
                                    response=httpx.Response(401))
    err_403 = httpx.HTTPStatusError("403", request=httpx.Request("GET", "http://x"),
                                    response=httpx.Response(403))
    assert auth._is_oci_auth_error(err_401)
    assert not auth._is_oci_auth_error(err_403)


# ---- call_with_oci_token_refresh ----


def test_refresh_retries_once_on_401_with_a_fresh_token(monkeypatch):
    minted = []

    def fake_mint(oci_repo, token, *, push, endpoint=None, use_cache=True):
        t = f"tok-{len(minted)}-cache{use_cache}"
        minted.append(use_cache)
        return t

    cleared = []
    monkeypatch.setattr(auth, "get_oci_bearer_token", fake_mint)
    monkeypatch.setattr(auth, "clear_oci_token_cache", lambda: cleared.append(1))

    seen = []

    def op(oci_token):
        seen.append(oci_token)
        if len(seen) == 1:
            raise RuntimeError("server returned 401 (Unauthorized)")
        return "ok"

    result = auth.call_with_oci_token_refresh("acme/model", "t", push=True, operation=op)

    assert result == "ok"
    assert len(seen) == 2, "must retry exactly once on a 401"
    assert cleared == [1], "the token cache must be cleared before re-minting"
    assert seen[0] != seen[1], "the retry must use a freshly minted token"
    assert minted == [True, False], "first mint may use cache; the refresh mint bypasses it"


def test_refresh_does_not_retry_a_non_401(monkeypatch):
    monkeypatch.setattr(auth, "get_oci_bearer_token", lambda *a, **k: "tok")
    seen = []

    def op(oci_token):
        seen.append(oci_token)
        raise RuntimeError("server returned 500 (boom)")

    with pytest.raises(RuntimeError, match="500"):
        auth.call_with_oci_token_refresh("acme/model", "t", push=True, operation=op)
    assert len(seen) == 1, "a non-401 must not consume the refresh retry"


def test_refresh_gives_up_after_a_second_401(monkeypatch):
    monkeypatch.setattr(auth, "get_oci_bearer_token", lambda *a, **k: "tok")
    monkeypatch.setattr(auth, "clear_oci_token_cache", lambda: None)
    seen = []

    def op(oci_token):
        seen.append(oci_token)
        raise RuntimeError("server returned 401 (Unauthorized)")

    with pytest.raises(RuntimeError, match="401"):
        auth.call_with_oci_token_refresh("acme/model", "t", push=True, operation=op)
    assert len(seen) == 2, "a fresh token that is also rejected is a real auth failure"


def test_refresh_uses_initial_token_on_first_attempt(monkeypatch):
    # snapshot_download passes a pre-resolved per-file token via `initial`; it must be
    # used as-is on the first attempt rather than minting a new one.
    minted = []
    monkeypatch.setattr(auth, "get_oci_bearer_token",
                        lambda *a, **k: minted.append(1) or "minted")
    seen = []
    auth.call_with_oci_token_refresh(
        "acme/model", "t", push=False, operation=lambda tok: seen.append(tok),
        initial="shared-token",
    )
    assert seen == ["shared-token"]
    assert minted == [], "initial token must skip the up-front mint"


# ---- wiring: hf_hub_download recovers from a native 401 ----


@respx.mock
def test_hf_hub_download_refreshes_token_on_native_401(monkeypatched_registry, monkeypatch, tmp_path):
    auth.clear_oci_token_cache()
    payload = b"Z" * 42
    repo = "acme/model"
    plain = {
        "schemaVersion": 2,
        "layers": [
            {
                "mediaType": "application/octet-stream",
                "size": len(payload),
                "digest": "sha256:" + "a" * 64,
                "annotations": {LAYER_TITLE_KEY: "file.txt"},
            }
        ],
    }
    token_route(respx.mock)
    respx.get(f"{MOCK_REGISTRY}/v2/{repo}/manifests/main").mock(
        return_value=httpx.Response(200, json=plain, headers={"Docker-Content-Digest": "sha256:" + "d" * 64})
    )

    calls = {"n": 0}

    def fake_native(*, dest_path, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            # Token expired mid-download — the Rust surface of a 401.
            raise RuntimeError("server returned 401 (Unauthorized)")
        with open(dest_path, "wb") as f:
            f.write(payload)
        return None

    monkeypatch.setattr(file_download, "download_file_native", fake_native)

    out = hf_hub_download(
        repo_id=repo, filename="file.txt", revision="main", cache_dir=str(tmp_path), token="tok"
    )
    assert open(out, "rb").read() == payload
    assert calls["n"] == 2, "the native 401 must trigger a token refresh + one retry"


def test_upload_file_401_retry_reuses_normalized_content_not_empty(monkeypatch):
    # Audit M2 correctness regression: a file-like input must be normalized ONCE,
    # OUTSIDE the retry, so a 401 retry re-uploads the SAME content — not an empty
    # temp file read from the already-exhausted stream. With the pre-fix code the
    # second attempt saw b"" (silent empty-blob corruption committed as success).
    auth.clear_oci_token_cache()
    monkeypatch.setattr(auth, "get_oci_bearer_token", lambda *a, **k: "tok")
    monkeypatch.setattr(file_upload, "fetch_manifest", lambda *a, **k: None)
    monkeypatch.setattr(
        file_upload, "_ensure_config_blob_uploaded", lambda *a, **k: ("sha256:" + "c" * 64, 2)
    )
    monkeypatch.setattr(file_upload, "_put_manifest", lambda *a, **k: SimpleNamespace(headers={}))
    monkeypatch.setattr(file_upload, "_build_commit_info", lambda *a, **k: "commit-ok")

    seen = []

    def fake_layers(file_path, *a, **k):
        with open(file_path, "rb") as fh:
            seen.append(fh.read())
        if len(seen) == 1:
            # Token expires during the blob PUT — the native 401 surface.
            raise RuntimeError("server returned 401 (Unauthorized)")
        return []

    monkeypatch.setattr(file_upload, "_upload_file_layers", fake_layers)

    result = file_upload.upload_file(
        path_or_fileobj=io.BytesIO(b"real content"),
        path_in_repo="model.bin",
        repo_id="acme/model",
        repo_type="model",
    )

    assert result == "commit-ok"
    assert len(seen) == 2, "the 401 must trigger exactly one retry"
    assert seen == [b"real content", b"real content"], (
        "the retry must re-upload the SAME normalized content, never the exhausted (empty) stream"
    )
