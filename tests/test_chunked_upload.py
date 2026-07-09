"""Chunked-upload orchestration + the group-aware manifest merge.

Two concerns:
  1. `_merge_layers` must treat a chunked file (pointer + K chunk layers) as one
     indivisible group — the data-loss regression the plan flags: a title-keyed
     merge would either collapse a chunked file to its pointer or wipe its chunk
     layers when an unrelated file is committed into the same repo.
  2. `upload_file` on a large file must emit the Option-B manifest (titled
     pointer + untitled chunks + artifactType + com.hippius.layout), and that
     manifest must round-trip back through the Phase 1 reader (`group_files`).

The native chunk/upload primitives are stubbed here (their real behavior is
covered in test_chunk_upload_native.py); this pins the Python assembly around them.
"""
from __future__ import annotations

import hashlib
import json

import httpx
import pytest
import respx

from hippius_hub import file_upload
from hippius_hub._oci import group_files
from hippius_hub.constants import (
    ARTIFACT_TYPE_CHUNKED,
    CHUNK_COUNT_KEY,
    CHUNK_MEDIA_TYPE,
    CHUNKED_LAYOUT,
    FILE_DIGEST_KEY,
    FILE_SIZE_KEY,
    LAYER_TITLE_KEY,
    LAYOUT_ANNOTATION_KEY,
    POINTER_MEDIA_TYPE,
)
from hippius_hub.errors import ManifestTooLargeError
from hippius_hub.file_upload import (
    _assemble_manifest,
    _merge_layers,
    _partition_groups,
    upload_file,
)

from tests.respx_fixtures import MOCK_REGISTRY, token_route


# ---- group-aware merge ----

def _pointer(title: str, nchunks: int) -> dict:
    return {
        "mediaType": POINTER_MEDIA_TYPE,
        "digest": "sha256:" + hashlib.sha256(title.encode()).hexdigest(),
        "size": 200,
        "annotations": {
            LAYER_TITLE_KEY: title,
            FILE_SIZE_KEY: "1000",
            FILE_DIGEST_KEY: "sha256:" + "f" * 64,
            CHUNK_COUNT_KEY: str(nchunks),
        },
    }


def _chunk(name: str) -> dict:
    return {"mediaType": CHUNK_MEDIA_TYPE, "digest": f"sha256:{name}", "size": 100}


def _plain(title: str) -> dict:
    return {
        "mediaType": "application/octet-stream",
        "digest": "sha256:" + "a" * 64,
        "size": 10,
        "annotations": {LAYER_TITLE_KEY: title},
    }


def _titles(layers):
    return [l.get("annotations", {}).get(LAYER_TITLE_KEY) for l in layers]


def test_partition_keeps_chunks_with_their_pointer():
    layers = [_pointer("big.bin", 2), _chunk("c0"), _chunk("c1"), _plain("readme")]
    groups = _partition_groups(layers)
    assert [t for t, _ in groups] == ["big.bin", "readme"]
    assert len(groups[0][1]) == 3  # pointer + 2 chunks
    assert len(groups[1][1]) == 1


def test_committing_unrelated_file_preserves_chunked_group():
    # THE data-loss regression: uploading small.txt into a repo holding chunked
    # big.bin must leave big.bin's pointer AND both chunk layers intact.
    existing = [_pointer("big.bin", 2), _chunk("c0"), _chunk("c1"), _plain("readme")]
    merged = _merge_layers(existing, [_plain("small.txt")])

    digests = [l["digest"] for l in merged]
    assert "sha256:c0" in digests and "sha256:c1" in digests  # chunks survived
    assert set(_titles(merged)) - {None} == {"big.bin", "readme", "small.txt"}
    # And they still parse as one logical file with 2 chunks.
    big = next(g for g in group_files({"layers": merged}) if g.title == "big.bin")
    assert big.is_chunked and len(big.chunks) == 2


def test_replacing_chunked_file_swaps_whole_group():
    existing = [_pointer("big.bin", 2), _chunk("old0"), _chunk("old1")]
    new = [_pointer("big.bin", 3), _chunk("new0"), _chunk("new1"), _chunk("new2")]
    merged = _merge_layers(existing, new)

    digests = [l["digest"] for l in merged]
    assert "sha256:old0" not in digests and "sha256:old1" not in digests  # no stale chunks
    assert digests.count("sha256:new0") == 1
    big = next(g for g in group_files({"layers": merged}) if g.title == "big.bin")
    assert len(big.chunks) == 3


def test_manifest_size_guard_accepts_normal():
    m = _assemble_manifest("sha256:" + "c" * 64, 2, [_plain("readme")], "msg", "")
    assert m["layers"] == [_plain("readme")]


def test_manifest_size_guard_rejects_oversize():
    # An artifact with tens of thousands of chunk layers exceeds the 4 MiB
    # registry manifest cap; assembling it must fail loudly before the PUT,
    # not with an opaque 400 after all blobs are uploaded.
    chunk = {"mediaType": CHUNK_MEDIA_TYPE, "size": 67108864, "digest": "sha256:" + "a" * 64}
    layers = [_pointer("big.bin", 40000)] + [dict(chunk) for _ in range(40000)]
    with pytest.raises(ManifestTooLargeError, match="registry limit"):
        _assemble_manifest("sha256:" + "c" * 64, 2, layers, "msg", "")


def test_delete_pattern_drops_whole_chunked_group():
    existing = [_pointer("big.bin", 2), _chunk("c0"), _chunk("c1"), _plain("readme")]
    merged = _merge_layers(existing, [], delete_titles={"big.bin"})
    assert _titles(merged) == ["readme"]
    assert all(l["mediaType"] != CHUNK_MEDIA_TYPE for l in merged)  # chunks gone too


# ---- upload orchestration → manifest shape ----

REPO = "acme/model"
CHUNK_METAS = [("aa" * 32, 0, 600), ("bb" * 32, 600, 400)]  # (hex, offset, size)
WHOLE_HEX = "cc" * 32


@respx.mock
def test_upload_large_file_emits_chunked_manifest(monkeypatch, tmp_path):
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "1")  # force the chunked path
    monkeypatch.setattr(file_upload, "chunk_and_hash_native", lambda path, avg: (WHOLE_HEX, CHUNK_METAS))
    monkeypatch.setattr(file_upload, "upload_blob_range_native", lambda **kw: None)
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)

    token_route(respx.mock)
    respx.head(url__regex=rf"{MOCK_REGISTRY}/v2/{REPO}/blobs/.*").mock(return_value=httpx.Response(404))
    respx.post(f"{MOCK_REGISTRY}/v2/{REPO}/blobs/uploads/").mock(
        return_value=httpx.Response(202, headers={"Location": f"{MOCK_REGISTRY}/v2/{REPO}/blobs/uploads/uuid"})
    )
    respx.put(url__startswith=f"{MOCK_REGISTRY}/v2/{REPO}/blobs/uploads/uuid").mock(return_value=httpx.Response(201))
    respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(return_value=httpx.Response(404))

    captured = {}

    def _capture(request):
        captured["manifest"] = json.loads(request.content)
        return httpx.Response(201, headers={"Docker-Content-Digest": "sha256:" + "b" * 64})

    respx.put(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(side_effect=_capture)

    src = tmp_path / "big.bin"
    src.write_bytes(b"payload")  # size only needs to clear the (lowered) threshold
    upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")

    manifest = captured["manifest"]
    # Artifact typing + layout marker present.
    assert manifest["artifactType"] == ARTIFACT_TYPE_CHUNKED
    assert manifest["annotations"][LAYOUT_ANNOTATION_KEY] == CHUNKED_LAYOUT
    assert manifest["config"]["mediaType"] == "application/vnd.oci.empty.v1+json"

    layers = manifest["layers"]
    assert layers[0]["mediaType"] == POINTER_MEDIA_TYPE
    assert layers[0]["annotations"][LAYER_TITLE_KEY] == "big.bin"
    assert layers[0]["annotations"][CHUNK_COUNT_KEY] == "2"
    assert layers[0]["annotations"][FILE_DIGEST_KEY] == f"sha256:{WHOLE_HEX}"
    assert [l["mediaType"] for l in layers[1:3]] == [CHUNK_MEDIA_TYPE, CHUNK_MEDIA_TYPE]
    assert LAYER_TITLE_KEY not in layers[1].get("annotations", {})  # chunks untitled
    assert [l["digest"] for l in layers[1:3]] == [f"sha256:{CHUNK_METAS[0][0]}", f"sha256:{CHUNK_METAS[1][0]}"]

    # The uploader's output round-trips back through the Phase 1 reader.
    (fg,) = [g for g in group_files(manifest) if g.title == "big.bin"]
    assert fg.is_chunked and len(fg.chunks) == 2
    assert fg.digest == f"sha256:{WHOLE_HEX}"


@respx.mock
def test_write_gate_forces_plain_for_large_file(monkeypatch, tmp_path):
    # A large file with HIPPIUS_CHUNKED_WRITE=0 must upload as a plain blob (the
    # rollout escape hatch), never touching the chunker.
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "1")
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "0")
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)

    def _boom(*a, **k):
        raise AssertionError("write gate off must skip chunking")

    monkeypatch.setattr(file_upload, "chunk_and_hash_native", _boom)
    monkeypatch.setattr(file_upload, "hash_file_native", lambda path: ("d" * 64, 9))
    monkeypatch.setattr(file_upload, "_ensure_blob_uploaded", lambda *a, **k: True)

    token_route(respx.mock)
    respx.head(url__regex=rf"{MOCK_REGISTRY}/v2/{REPO}/blobs/.*").mock(return_value=httpx.Response(200))
    respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(return_value=httpx.Response(404))
    captured = {}
    respx.put(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(
        side_effect=lambda request: (captured.__setitem__("m", json.loads(request.content)),
                                      httpx.Response(201, headers={"Docker-Content-Digest": "sha256:" + "b" * 64}))[1]
    )

    src = tmp_path / "big.bin"
    src.write_bytes(b"a-large-payload")
    upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")

    manifest = captured["m"]
    assert "artifactType" not in manifest
    assert manifest["layers"][0]["mediaType"] == "application/octet-stream"


@respx.mock
def test_upload_small_file_stays_plain(monkeypatch, tmp_path):
    # Below the threshold: no chunking, no artifactType, no layout annotation —
    # byte-identical to the pre-chunking output (cross-dedup with old artifacts).
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)

    def _boom(*a, **k):
        raise AssertionError("small file must not be chunked")

    monkeypatch.setattr(file_upload, "chunk_and_hash_native", _boom)
    monkeypatch.setattr(file_upload, "hash_file_native", lambda path: ("d" * 64, 5))
    monkeypatch.setattr(file_upload, "_ensure_blob_uploaded", lambda *a, **k: True)

    token_route(respx.mock)
    respx.head(url__regex=rf"{MOCK_REGISTRY}/v2/{REPO}/blobs/.*").mock(return_value=httpx.Response(200))
    respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(return_value=httpx.Response(404))

    captured = {}
    respx.put(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(
        side_effect=lambda request: (captured.__setitem__("m", json.loads(request.content)),
                                      httpx.Response(201, headers={"Docker-Content-Digest": "sha256:" + "b" * 64}))[1]
    )

    src = tmp_path / "readme.txt"
    src.write_bytes(b"hello")
    upload_file(path_or_fileobj=str(src), path_in_repo="readme.txt", repo_id=REPO, token="tok")

    manifest = captured["m"]
    assert "artifactType" not in manifest
    assert LAYOUT_ANNOTATION_KEY not in manifest.get("annotations", {})
    assert manifest["layers"][0]["mediaType"] == "application/octet-stream"
