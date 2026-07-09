"""Python routing glue for chunked downloads in `hf_hub_download`.

Verifies the client dispatches a chunked file to the native chunk-assembler with
the right per-chunk URLs/digests/sizes and lands it in the cache layout, while a
plain file still takes the single-blob path untouched. The native call itself is
stubbed (its real behavior is covered end-to-end in
`test_chunked_download_native.py`); here we pin the wiring around it.
"""
from __future__ import annotations

import hashlib
import os

import httpx
import pytest
import respx

from hippius_hub import file_download
from hippius_hub.constants import (
    CHUNK_COUNT_KEY,
    CHUNK_MEDIA_TYPE,
    CHUNKED_LAYOUT,
    FILE_DIGEST_KEY,
    FILE_SIZE_KEY,
    LAYER_TITLE_KEY,
    LAYOUT_ANNOTATION_KEY,
    POINTER_MEDIA_TYPE,
)
from hippius_hub.file_download import hf_hub_download

from tests.respx_fixtures import MOCK_REGISTRY, token_route


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


REPO = "acme/model"
CHUNKS = [b"X" * 1000, b"Y" * 1500]
CHUNK_DIGESTS = [_sha(c) for c in CHUNKS]
FILE_BYTES = b"".join(CHUNKS)
FILE_DIGEST = _sha(FILE_BYTES)


def _chunked_manifest() -> dict:
    layers = [
        {
            "mediaType": POINTER_MEDIA_TYPE,
            "size": 200,
            "digest": "sha256:" + _sha(b"pointer"),
            "annotations": {
                LAYER_TITLE_KEY: "big.bin",
                FILE_SIZE_KEY: str(len(FILE_BYTES)),
                FILE_DIGEST_KEY: f"sha256:{FILE_DIGEST}",
                CHUNK_COUNT_KEY: str(len(CHUNKS)),
            },
        },
    ]
    layers += [
        {"mediaType": CHUNK_MEDIA_TYPE, "size": len(c), "digest": f"sha256:{d}"}
        for c, d in zip(CHUNKS, CHUNK_DIGESTS)
    ]
    return {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "annotations": {LAYOUT_ANNOTATION_KEY: CHUNKED_LAYOUT},
        "layers": layers,
    }


def _mock_manifest(manifest: dict):
    token_route(respx.mock)
    respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(
        return_value=httpx.Response(
            200, json=manifest, headers={"Docker-Content-Digest": "sha256:" + "d" * 64}
        )
    )


@respx.mock
def test_chunked_file_routes_to_native_assembler(monkeypatched_registry, monkeypatch, tmp_path):
    _mock_manifest(_chunked_manifest())

    calls = []

    def fake_native(*, urls, chunk_digests, chunk_sizes, dest_path, file_digest, auth_token, max_concurrent):
        calls.append(
            {"urls": urls, "chunk_digests": chunk_digests, "chunk_sizes": chunk_sizes,
             "file_digest": file_digest}
        )
        with open(dest_path, "wb") as f:
            f.write(FILE_BYTES)
        return None

    monkeypatch.setattr(file_download, "download_chunks_native", fake_native)

    out = hf_hub_download(
        repo_id=REPO, filename="big.bin", revision="main",
        cache_dir=str(tmp_path), token="tok",
    )

    # The native assembler was invoked with per-chunk URLs, bare-hex digests, and sizes.
    assert len(calls) == 1
    assert calls[0]["urls"] == [f"{MOCK_REGISTRY}/v2/{REPO}/blobs/sha256:{d}" for d in CHUNK_DIGESTS]
    assert calls[0]["chunk_digests"] == CHUNK_DIGESTS
    assert calls[0]["chunk_sizes"] == [len(c) for c in CHUNKS]
    # Whole-file digest is ALWAYS passed for chunked assembly (the ordering check),
    # not gated on HIPPIUS_VERIFY_HASH.
    assert calls[0]["file_digest"] == FILE_DIGEST

    # The assembled file is present with the right bytes...
    assert open(out, "rb").read() == FILE_BYTES
    # ...and cached under the whole-file digest (chunked files dedup on disk with
    # identical plain content).
    blob = os.path.join(tmp_path, "models--acme--model", "blobs", f"sha256:{FILE_DIGEST}")
    assert os.path.exists(blob)


@respx.mock
def test_chunked_file_to_local_dir(monkeypatched_registry, monkeypatch, tmp_path):
    _mock_manifest(_chunked_manifest())
    monkeypatch.setattr(
        file_download,
        "download_chunks_native",
        lambda **kw: open(kw["dest_path"], "wb").write(FILE_BYTES) and None,
    )

    local = tmp_path / "dl"
    out = hf_hub_download(
        repo_id=REPO, filename="big.bin", revision="main",
        local_dir=str(local), token="tok",
    )
    assert out == str(local / "big.bin")
    assert (local / "big.bin").read_bytes() == FILE_BYTES


@respx.mock
def test_plain_file_does_not_touch_chunk_assembler(monkeypatched_registry, monkeypatch, tmp_path):
    # A plain (pre-chunking) manifest must keep the single-blob Range path and
    # never call the chunk assembler.
    payload = b"Z" * 42
    plain = {
        "schemaVersion": 2,
        "layers": [
            {
                "mediaType": "application/octet-stream",
                "size": len(payload),
                "digest": f"sha256:{_sha(payload)}",
                "annotations": {LAYER_TITLE_KEY: "file.txt"},
            }
        ],
    }
    _mock_manifest(plain)

    def boom(**_kw):
        raise AssertionError("chunk assembler must not run for a plain file")

    monkeypatch.setattr(file_download, "download_chunks_native", boom)
    monkeypatch.setattr(
        file_download,
        "download_file_native",
        lambda **kw: open(kw["dest_path"], "wb").write(payload) and None,
    )

    out = hf_hub_download(
        repo_id=REPO, filename="file.txt", revision="main",
        cache_dir=str(tmp_path), token="tok",
    )
    assert open(out, "rb").read() == payload
