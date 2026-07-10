"""Chunked-v2 (pack layout) upload orchestration — offline.

Pins the Python assembly around the native pack primitives (which are stubbed
here; their real byte behavior is covered by the staging e2e). Verifies that a
large-file upload with `HIPPIUS_CHUNKED_LAYOUT=v2` emits the pack manifest
(titled pointer.v2 + untitled pack layers + artifactType.v2 + layout=chunked-v2),
that the pointer blob maps chunks to packs, that dedup against a prior revision
re-uploads only new chunks, and that the manifest round-trips through group_files.
"""
from __future__ import annotations

import hashlib
import json
import threading
import time

import httpx
import pytest
import respx

from hippius_hub import file_upload
from hippius_hub._oci import group_files, parse_pointer_v2
from hippius_hub.constants import (
    ARTIFACT_TYPE_CHUNKED_V2,
    CHUNKED_LAYOUT_V2,
    LAYOUT_ANNOTATION_KEY,
    PACK_MEDIA_TYPE,
    POINTER_MEDIA_TYPE_V2,
)
from hippius_hub.file_upload import upload_file

from tests.respx_fixtures import MOCK_REGISTRY, token_route

REPO = "acme/model"

# Two 4 MiB-ish chunks (hex, offset, size); whole file = 100 bytes for the test.
CHUNK_METAS = [("a" * 64, 0, 40), ("b" * 64, 40, 60)]
WHOLE_HEX = "c" * 64


def _pack_digest(ranges) -> str:
    # Deterministic stand-in for the real content digest so the pointer/manifest
    # are stable; the mock never reads bytes, only records which ranges packed.
    return hashlib.sha256(repr(list(ranges)).encode()).hexdigest()


def _wire_registry(monkeypatch, captured, *, existing_manifest=None):
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)
    monkeypatch.setattr(file_upload, "chunk_and_hash_native", lambda path, avg: (WHOLE_HEX, CHUNK_METAS))

    packs_seen = []

    def _fake_pack(uploads_url, path, ranges, auth_token):
        packs_seen.append(list(ranges))
        return _pack_digest(ranges)

    monkeypatch.setattr(file_upload, "pack_upload_native", _fake_pack)

    token_route(respx.mock)
    # Every blob HEAD misses so the pointer/config blobs upload; POST+PUT succeed.
    respx.head(url__regex=rf"{MOCK_REGISTRY}/v2/{REPO}/blobs/.*").mock(return_value=httpx.Response(404))
    respx.post(f"{MOCK_REGISTRY}/v2/{REPO}/blobs/uploads/").mock(
        return_value=httpx.Response(202, headers={"Location": f"{MOCK_REGISTRY}/v2/{REPO}/blobs/uploads/uuid"})
    )
    put_bodies = []

    def _capture_put(request):
        put_bodies.append(request.content)
        return httpx.Response(201)

    respx.put(url__startswith=f"{MOCK_REGISTRY}/v2/{REPO}/blobs/uploads/uuid").mock(side_effect=_capture_put)
    if existing_manifest is None:
        respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(return_value=httpx.Response(404))
    else:
        respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(
            return_value=httpx.Response(
                200,
                headers={"Docker-Content-Digest": "sha256:" + "e" * 64},
                json=existing_manifest,
            )
        )

    def _capture_manifest(request):
        captured["manifest"] = json.loads(request.content)
        return httpx.Response(201, headers={"Docker-Content-Digest": "sha256:" + "f" * 64})

    respx.put(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(side_effect=_capture_manifest)
    return packs_seen, put_bodies


@respx.mock
def test_v2_upload_emits_pack_manifest(monkeypatch, tmp_path):
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "1")
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "1")
    monkeypatch.setenv("HIPPIUS_CHUNKED_LAYOUT", "v2")
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", "1000")  # both chunks fit one pack
    captured = {}
    packs_seen, put_bodies = _wire_registry(monkeypatch, captured)

    src = tmp_path / "big.bin"
    src.write_bytes(b"x" * 100)
    upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")

    manifest = captured["manifest"]
    assert manifest["artifactType"] == ARTIFACT_TYPE_CHUNKED_V2
    assert manifest["annotations"][LAYOUT_ANNOTATION_KEY] == CHUNKED_LAYOUT_V2
    pointers = [m for m in manifest["layers"] if m["mediaType"] == POINTER_MEDIA_TYPE_V2]
    packs = [m for m in manifest["layers"] if m["mediaType"] == PACK_MEDIA_TYPE]
    assert len(pointers) == 1
    assert len(packs) == 1, "both chunks fit one pack"
    # Both chunk ranges went into the single pack, in file order.
    assert packs_seen == [[(0, 40), (40, 60)]]

    # The manifest round-trips through the reader as one chunked-v2 file.
    (grp,) = group_files(manifest)
    assert grp.layout == CHUNKED_LAYOUT_V2 and grp.is_chunked and grp.size == 100

    # The pointer blob maps both chunks into that one pack.
    ptr_blob = next(b for b in put_bodies if b.startswith(b'{"chunks"') or b'"chunked-v2"' in b)
    refs = parse_pointer_v2(ptr_blob)
    assert [r.chunk_digest for r in refs] == ["sha256:" + "a" * 64, "sha256:" + "b" * 64]
    assert len({r.pack_digest for r in refs}) == 1
    assert refs[0].pack_offset == 0 and refs[1].pack_offset == 40


@respx.mock
def test_v2_reupload_dedups_unchanged_chunk(monkeypatch, tmp_path):
    # Prior revision already stored chunk "a" in pack P0. Re-upload references it by
    # range and packs only the (new) chunk "b" — the "upload only missing bytes" win.
    p0 = "sha256:" + _pack_digest([(0, 40)])
    prior_pointer = json.dumps({
        "version": "chunked-v2",
        "file": {"size": 40, "digest": "sha256:" + "d" * 64},
        "chunks": [{"digest": "sha256:" + "a" * 64, "size": 40, "pack": p0, "offset": 0}],
    }).encode()
    prior_ptr_digest = "sha256:" + hashlib.sha256(prior_pointer).hexdigest()
    existing = {
        "schemaVersion": 2,
        "layers": [
            {"mediaType": POINTER_MEDIA_TYPE_V2, "digest": prior_ptr_digest, "size": len(prior_pointer),
             "annotations": {"org.opencontainers.image.title": "big.bin",
                             "com.hippius.file.size": "40", "com.hippius.file.digest": "sha256:" + "d" * 64,
                             "com.hippius.chunk.count": "1"}},
            {"mediaType": PACK_MEDIA_TYPE, "digest": p0, "size": 40},
        ],
        "annotations": {LAYOUT_ANNOTATION_KEY: CHUNKED_LAYOUT_V2},
    }

    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "1")
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "1")
    monkeypatch.setenv("HIPPIUS_CHUNKED_LAYOUT", "v2")
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", "1000")
    captured = {}
    packs_seen, put_bodies = _wire_registry(monkeypatch, captured, existing_manifest=existing)
    # The prior pointer blob is fetched to build the dedup index.
    respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/blobs/{prior_ptr_digest}").mock(
        return_value=httpx.Response(200, content=prior_pointer)
    )

    src = tmp_path / "big.bin"
    src.write_bytes(b"x" * 100)
    upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")

    # Only the NEW chunk "b" (offset 40, size 60) was packed; chunk "a" reused by range.
    assert packs_seen == [[(40, 60)]]
    manifest = captured["manifest"]
    (grp,) = group_files(manifest)
    assert grp.layout == CHUNKED_LAYOUT_V2
    # Pointer references two packs now: the reused P0 and the new pack for "b".
    ptr_blob = next(b for b in put_bodies if b'"chunked-v2"' in b)
    refs = parse_pointer_v2(ptr_blob)
    assert refs[0].pack_digest == p0  # "a" reused from the old pack
    assert refs[1].pack_digest != p0  # "b" in a freshly uploaded pack


@respx.mock
def test_v2_pack_uploads_respect_inflight_cap(monkeypatch, tmp_path):
    """The shared gate bounds concurrent pack uploads to the cap even when the
    per-file worker pool would allow more — this is what stops a folder upload's
    nested file×pack parallelism from multiplying resident pack memory. With two
    packs, a 4-wide pool, and cap=1, the two uploads MUST serialize."""
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "1")
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "1")
    monkeypatch.setenv("HIPPIUS_CHUNKED_LAYOUT", "v2")
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", "40")        # chunk "a" (40) closes pack 0 -> two packs
    monkeypatch.setenv("HIPPIUS_UPLOAD_WORKERS", "4")    # pool would run both at once...
    monkeypatch.setenv("HIPPIUS_MAX_INFLIGHT_PACKS", "1")  # ...but the cap serializes them
    captured = {}
    _wire_registry(monkeypatch, captured)

    state = {"cur": 0, "max": 0}
    lock = threading.Lock()

    def _slow_pack(uploads_url, path, ranges, auth_token):
        with lock:
            state["cur"] += 1
            state["max"] = max(state["max"], state["cur"])
        time.sleep(0.05)  # hold the slot so a concurrency breach would overlap here
        with lock:
            state["cur"] -= 1
        return _pack_digest(ranges)

    monkeypatch.setattr(file_upload, "pack_upload_native", _slow_pack)

    src = tmp_path / "big.bin"
    src.write_bytes(b"x" * 100)
    upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")

    # Two packs were produced (so concurrency was possible) but never overlapped.
    assert len(captured["manifest"]["layers"]) >= 3  # pointer + 2 packs
    assert state["max"] == 1, "cap=1 must serialize pack uploads despite a 4-wide pool"
