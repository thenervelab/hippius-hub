"""Python routing glue for chunked-v2 downloads in `hf_hub_download`.

Verifies the client dispatches a chunked-v2 file to the native pack assembler with
the right pack URLs/sizes/chunk targets (after fetching and parsing the pointer
blob) and lands it in the cache layout, while a plain file still takes the
single-blob path untouched. The native call itself is stubbed (its real byte
behavior is covered end-to-end by the staging v2 round-trip); here we pin the
wiring around it.
"""
from __future__ import annotations

import hashlib
import json
import os

import httpx
import pytest
import respx

from hippius_hub import file_download
from hippius_hub.constants import (
    CHUNK_COUNT_KEY,
    CHUNKED_LAYOUT_V2,
    FILE_DIGEST_KEY,
    FILE_SIZE_KEY,
    LAYER_TITLE_KEY,
    LAYOUT_ANNOTATION_KEY,
    PACK_MEDIA_TYPE,
    POINTER_MEDIA_TYPE_V2,
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
# Both chunks share one pack whose content is the concatenation, in file order.
PACK_DIGEST = "sha256:" + _sha(FILE_BYTES)
POINTER_DIGEST = "sha256:" + _sha(b"pointer")


def _pointer_blob() -> bytes:
    doc = {
        "version": CHUNKED_LAYOUT_V2,
        "file": {"size": len(FILE_BYTES), "digest": f"sha256:{FILE_DIGEST}"},
        "chunks": [
            {"digest": f"sha256:{CHUNK_DIGESTS[0]}", "size": 1000, "pack": PACK_DIGEST, "offset": 0},
            {"digest": f"sha256:{CHUNK_DIGESTS[1]}", "size": 1500, "pack": PACK_DIGEST, "offset": 1000},
        ],
    }
    return json.dumps(doc).encode()


def _chunked_manifest() -> dict:
    return {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "annotations": {LAYOUT_ANNOTATION_KEY: CHUNKED_LAYOUT_V2},
        "layers": [
            {
                "mediaType": POINTER_MEDIA_TYPE_V2,
                "size": 200,
                "digest": POINTER_DIGEST,
                "annotations": {
                    LAYER_TITLE_KEY: "big.bin",
                    FILE_SIZE_KEY: str(len(FILE_BYTES)),
                    FILE_DIGEST_KEY: f"sha256:{FILE_DIGEST}",
                    CHUNK_COUNT_KEY: str(len(CHUNKS)),
                },
            },
            {"mediaType": PACK_MEDIA_TYPE, "size": len(FILE_BYTES), "digest": PACK_DIGEST},
        ],
    }


def _mock_chunked():
    token_route(respx.mock)
    respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(
        return_value=httpx.Response(
            200, json=_chunked_manifest(), headers={"Docker-Content-Digest": "sha256:" + "d" * 64}
        )
    )
    respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/blobs/{POINTER_DIGEST}").mock(
        return_value=httpx.Response(200, content=_pointer_blob())
    )


@respx.mock
def test_chunked_file_routes_to_native_pack_assembler(monkeypatched_registry, monkeypatch, tmp_path):
    _mock_chunked()

    calls = []

    def fake_native(*, pack_urls, pack_sizes, pack_chunks, dest_path, total_size, file_digest, auth_token, max_concurrent):
        calls.append({"pack_urls": pack_urls, "pack_sizes": pack_sizes,
                      "pack_chunks": pack_chunks, "total_size": total_size, "file_digest": file_digest})
        with open(dest_path, "wb") as f:
            f.write(FILE_BYTES)
        return None

    monkeypatch.setattr(file_download, "download_packs_native", fake_native)

    out = hf_hub_download(
        repo_id=REPO, filename="big.bin", revision="main",
        cache_dir=str(tmp_path), token="tok",
    )

    # One pack, addressed by its content digest; both chunks coalesced into it with
    # their (pack_offset, size, file_offset, chunk_hex) targets in file order.
    assert len(calls) == 1
    assert calls[0]["pack_urls"] == [f"{MOCK_REGISTRY}/v2/{REPO}/blobs/{PACK_DIGEST}"]
    assert calls[0]["pack_sizes"] == [len(FILE_BYTES)]
    assert calls[0]["pack_chunks"] == [[(0, 1000, 0, CHUNK_DIGESTS[0]), (1000, 1500, 1000, CHUNK_DIGESTS[1])]]
    assert calls[0]["total_size"] == len(FILE_BYTES)
    # Whole-file digest is ALWAYS passed (the cross-pack ordering check), not gated
    # on HIPPIUS_VERIFY_HASH.
    assert calls[0]["file_digest"] == FILE_DIGEST

    assert open(out, "rb").read() == FILE_BYTES
    blob = os.path.join(tmp_path, "models--acme--model", "blobs", f"sha256:{FILE_DIGEST}")
    assert os.path.exists(blob)


@respx.mock
def test_chunked_file_to_local_dir(monkeypatched_registry, monkeypatch, tmp_path):
    _mock_chunked()
    monkeypatch.setattr(
        file_download,
        "download_packs_native",
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
def test_plain_file_does_not_touch_pack_assembler(monkeypatched_registry, monkeypatch, tmp_path):
    # A plain (pre-chunking) manifest must keep the single-blob Range path and
    # never call the pack assembler.
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
    token_route(respx.mock)
    respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(
        return_value=httpx.Response(200, json=plain, headers={"Docker-Content-Digest": "sha256:" + "d" * 64})
    )

    def boom(**_kw):
        raise AssertionError("pack assembler must not run for a plain file")

    monkeypatch.setattr(file_download, "download_packs_native", boom)
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
