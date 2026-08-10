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
import random
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


class _FakeChunkStream:
    """Stub of the native ChunkStream: replays canned (hex, offset, len) batches.

    Honors the real drain-before-finish contract (finish() before next_batch
    returned None raises) so orchestration bugs in the streaming upload loop
    surface here rather than only against the real extension."""

    def __init__(self, batches, whole_hex):
        self._batches = [list(b) for b in batches]
        self._whole = whole_hex
        self._drained = False

    def next_batch(self):
        if self._batches:
            return self._batches.pop(0)
        self._drained = True
        return None

    def finish(self):
        if not self._drained:
            raise RuntimeError("chunk stream not finished; drain next_batch to None first")
        return self._whole


def _pack_digest(ranges) -> str:
    # Deterministic stand-in for the real content digest so the pointer/manifest
    # are stable; the mock never reads bytes, only records which ranges packed.
    return hashlib.sha256(repr(list(ranges)).encode()).hexdigest()


def _wire_registry(monkeypatch, captured, *, existing_manifest=None, stub_chunks=True):
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)
    if stub_chunks:
        monkeypatch.setattr(
            file_upload, "chunk_stream_native",
            lambda path, avg: _FakeChunkStream([CHUNK_METAS], WHOLE_HEX),
        )

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


@respx.mock
def test_v2_folder_reupload_dedups_unchanged_chunk(monkeypatch, tmp_path):
    # #7: upload_folder must build the dedup index from the prior revision too
    # (upload_file already did; folders silently didn't). A folder re-upload should
    # reference chunk "a" by range and pack only the new chunk "b" -- the same
    # "upload only missing bytes" win, previously disabled for the folder path.
    from hippius_hub.file_upload import upload_folder

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
    packs_seen, _put_bodies = _wire_registry(monkeypatch, captured, existing_manifest=existing)
    respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/blobs/{prior_ptr_digest}").mock(
        return_value=httpx.Response(200, content=prior_pointer)
    )

    folder = tmp_path / "src"
    folder.mkdir()
    (folder / "big.bin").write_bytes(b"x" * 100)
    upload_folder(repo_id=REPO, folder_path=str(folder), token="tok")

    # Only the NEW chunk "b" (40, 60) was packed; chunk "a" was reused by range.
    assert packs_seen == [[(40, 60)]]


def test_build_dedup_index_excludes_a_lost_pack(monkeypatch):
    # The blob-loss self-heal for a REUSED pack: when the registry reports a pack
    # gone (BLOB_UNKNOWN), the next commit attempt must drop that pack from the
    # dedup index so its chunks re-pack, instead of re-referencing the missing pack
    # and failing identically. Two prior packs P0 (chunk "a") and P1 (chunk "b").
    import types

    p0 = "sha256:" + "a0" * 32
    p1 = "sha256:" + "b1" * 32
    prior_pointer = json.dumps({
        "version": "chunked-v2",
        "file": {"size": 80, "digest": "sha256:" + "d" * 64},
        "chunks": [
            {"digest": "sha256:" + "a" * 64, "size": 40, "pack": p0, "offset": 0},
            {"digest": "sha256:" + "b" * 64, "size": 40, "pack": p1, "offset": 0},
        ],
    }).encode()
    prior_ptr_digest = "sha256:" + hashlib.sha256(prior_pointer).hexdigest()
    manifest = {
        "schemaVersion": 2,
        "layers": [
            {"mediaType": POINTER_MEDIA_TYPE_V2, "digest": prior_ptr_digest,
             "size": len(prior_pointer),
             "annotations": {"org.opencontainers.image.title": "big.bin",
                             "com.hippius.file.size": "80",
                             "com.hippius.file.digest": "sha256:" + "d" * 64,
                             "com.hippius.chunk.count": "2"}},
            {"mediaType": PACK_MEDIA_TYPE, "digest": p0, "size": 40},
            {"mediaType": PACK_MEDIA_TYPE, "digest": p1, "size": 40},
        ],
        "annotations": {LAYOUT_ANNOTATION_KEY: CHUNKED_LAYOUT_V2},
    }
    existing = types.SimpleNamespace(manifest=manifest)
    # Serve the pointer blob from memory so the index builds without a network.
    monkeypatch.setattr(file_upload, "_fetch_blob", lambda *a, **k: prior_pointer)

    # No exclusion: both chunks are reusable and both pack sizes are known.
    index, sizes = file_upload._build_dedup_index(existing, MOCK_REGISTRY, REPO, "tok")
    assert set(sizes) == {p0, p1}
    assert index["sha256:" + "a" * 64][0] == p0
    assert index["sha256:" + "b" * 64][0] == p1

    # Exclude the lost pack P1: its chunk "b" drops from the index (so it re-packs)
    # and its size drops from pack_sizes (so it is not re-listed); P0 is untouched.
    index2, sizes2 = file_upload._build_dedup_index(
        existing, MOCK_REGISTRY, REPO, "tok", exclude_packs=frozenset({p1})
    )
    assert set(sizes2) == {p0}
    assert ("sha256:" + "a" * 64) in index2
    assert ("sha256:" + "b" * 64) not in index2


@respx.mock
def test_lost_reused_pack_heals_to_a_successful_commit(monkeypatch, tmp_path):
    # End-to-end proof of the self-heal (not just the index unit): the prior revision
    # stored chunk "a" in pack P0. On re-upload "a" dedups against P0 and only "b" is
    # packed — but the registry has LOST P0 and rejects any manifest still referencing
    # it (MANIFEST_BLOB_UNKNOWN). The heal must exclude P0, re-pack "a" into a fresh
    # pack, and the SECOND commit (no longer naming P0) must SUCCEED.
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "1")
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "1")
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", "1000")       # both chunks fit one pack
    monkeypatch.setenv("HIPPIUS_MANIFEST_PUT_RETRIES", "1")  # keep the reject loop short
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)
    monkeypatch.setattr("hippius_hub.file_upload.time.sleep", lambda _s: None)
    monkeypatch.setattr(
        file_upload, "chunk_stream_native",
        lambda path, avg: _FakeChunkStream([CHUNK_METAS], WHOLE_HEX),
    )

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

    token_route(respx.mock)
    respx.head(url__regex=rf"{MOCK_REGISTRY}/v2/{REPO}/blobs/.*").mock(return_value=httpx.Response(404))
    respx.post(f"{MOCK_REGISTRY}/v2/{REPO}/blobs/uploads/").mock(
        return_value=httpx.Response(202, headers={"Location": f"{MOCK_REGISTRY}/v2/{REPO}/blobs/uploads/uuid"}))
    respx.put(url__startswith=f"{MOCK_REGISTRY}/v2/{REPO}/blobs/uploads/uuid").mock(return_value=httpx.Response(201))
    respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(
        return_value=httpx.Response(200, headers={"Docker-Content-Digest": "sha256:" + "e" * 64}, json=existing))
    respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/blobs/{prior_ptr_digest}").mock(
        return_value=httpx.Response(200, content=prior_pointer))

    packs_seen = []
    monkeypatch.setattr(file_upload, "pack_upload_native",
                        lambda uploads_url, path, ranges, auth_token: (packs_seen.append(list(ranges)) or _pack_digest(ranges)))

    manifests = []

    def _manifest_put(request):
        m = json.loads(request.content)
        manifests.append(m)
        # The registry lost P0: reject while the manifest still references it, accept
        # once the heal has re-packed "a" elsewhere and dropped the P0 reference.
        if p0 in {layer["digest"] for layer in m["layers"]}:
            return httpx.Response(400, json={"errors": [{"code": "MANIFEST_BLOB_UNKNOWN", "detail": p0}]})
        return httpx.Response(201, headers={"Docker-Content-Digest": "sha256:" + "f" * 64})

    respx.put(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(side_effect=_manifest_put)

    src = tmp_path / "big.bin"
    src.write_bytes(b"x" * 100)
    info = file_upload.upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")

    assert info is not None, "the upload must succeed after the heal, not raise"
    # First attempt packed only the new chunk "b" (a reused from the now-lost P0);
    # after exclusion, the retry re-packed "a" too (both chunks into one fresh pack).
    assert [(40, 60)] in packs_seen, "first attempt packs only the new chunk b"
    assert [(0, 40), (40, 60)] in packs_seen, "the heal re-packs the lost pack's chunk a"
    # The committed manifest no longer references the lost pack.
    assert p0 not in {layer["digest"] for layer in manifests[-1]["layers"]}, \
        "the healed manifest must drop the lost pack"


# ---- streaming pipeline (B3.4): packs upload while chunking runs ----

@respx.mock
def test_v2_streaming_pipeline_matches_batch_plan(monkeypatch, tmp_path):
    """The streamed path (REAL native ChunkStream feeding PackAccumulator) must
    pack and point identically to the batch reference (chunk_and_hash_native +
    plan_packs): multiple packs completed mid-stream, multiple 64-chunk batches,
    a final partial pack from finish(), digests aligned pack-for-pack."""
    from hippius_hub._packing import plan_packs
    from hippius_hub.hippius_core import chunk_and_hash_native

    avg, pack_size = 32 * 1024, 512 * 1024
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "1")
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "1")
    monkeypatch.setenv("HIPPIUS_CDC_AVG_SIZE", str(avg))
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", str(pack_size))
    captured = {}
    packs_seen, put_bodies = _wire_registry(monkeypatch, captured, stub_chunks=False)

    src = tmp_path / "big.bin"
    src.write_bytes(random.Random(11).randbytes(5 * 1024 * 1024))
    upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")

    whole_hex, metas = chunk_and_hash_native(str(src), avg)
    ref = plan_packs([(f"sha256:{h}", s, o) for h, o, s in metas], {}, pack_size)
    # Fixture sanity: the streaming machinery is actually exercised.
    assert len(ref.new_packs) >= 3, "fixture must span multiple packs"
    assert len(metas) > 64, "fixture must span multiple stream batches"

    # Every planned pack uploaded exactly once (completion order may vary).
    assert sorted(map(tuple, packs_seen)) == sorted(np.ranges for np in ref.new_packs)

    # The pointer pins digest<->pack alignment: chunk i references the digest of
    # ITS planned pack at its planned offset — order-independent of upload timing.
    ptr_blob = next(b for b in put_bodies if b'"chunked-v2"' in b)
    refs = parse_pointer_v2(ptr_blob)
    assert [r.chunk_digest for r in refs] == [f"sha256:{h}" for h, _o, _s in metas]
    expected_digests = ["sha256:" + _pack_digest(list(np.ranges)) for np in ref.new_packs]
    assert [r.pack_digest for r in refs] == [
        expected_digests[pc.new_pack_index] for pc in ref.planned
    ]
    assert [r.pack_offset for r in refs] == [pc.pack_offset for pc in ref.planned]

    # Manifest annotations come from streamed counters, not a materialized list.
    ptr_layer = next(
        m for m in captured["manifest"]["layers"] if m["mediaType"] == POINTER_MEDIA_TYPE_V2
    )
    assert ptr_layer["annotations"]["com.hippius.chunk.count"] == str(len(metas))
    assert ptr_layer["annotations"]["com.hippius.file.digest"] == f"sha256:{whole_hex}"


@respx.mock
def test_v2_failed_pack_upload_propagates_without_hanging(monkeypatch, tmp_path):
    """A pack-upload failure must surface as the ORIGINAL exception (callers see
    the same shape the batch path raised) and must not deadlock the executor or
    the stream — bounded here by a hard deadline on a worker thread."""
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "1")
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "1")
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", "40")  # chunk "a" closes pack 0 -> two packs
    captured = {}
    _wire_registry(monkeypatch, captured)

    def _boom(uploads_url, path, ranges, auth_token):
        raise RuntimeError("pack exploded")

    monkeypatch.setattr(file_upload, "pack_upload_native", _boom)

    src = tmp_path / "big.bin"
    src.write_bytes(b"x" * 100)

    outcome = {}

    def _target():
        try:
            upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")
            outcome["value"] = "unexpected success"
        except BaseException as exc:  # the assertion below pins the exact type
            outcome["error"] = exc

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(30)
    assert not t.is_alive(), "a failed pack upload must not hang the upload"
    assert isinstance(outcome.get("error"), RuntimeError)
    assert "pack exploded" in str(outcome["error"])
    assert "manifest" not in captured, "no manifest may be committed after a failed pack"


@respx.mock
def test_v2_streaming_duplicate_chunks_pack_twice(monkeypatch, tmp_path):
    """No self-dedup, held end-to-end through the stream: a digest occurring twice
    within one file (absent from the dedup index) is packed twice. Split across
    two batches, with the pack under HIPPIUS_PACK_SIZE, this also pins the
    final-partial-pack path (submitted from new_packs[n_emitted:] after finish)."""
    dup_metas = [("a" * 64, 0, 40), ("a" * 64, 40, 40)]
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "1")
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "1")
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", "1000")
    captured = {}
    packs_seen, put_bodies = _wire_registry(monkeypatch, captured)
    monkeypatch.setattr(
        file_upload, "chunk_stream_native",
        lambda path, avg: _FakeChunkStream([dup_metas[:1], dup_metas[1:]], WHOLE_HEX),
    )

    src = tmp_path / "big.bin"
    src.write_bytes(b"x" * 80)
    upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")

    # Both occurrences packed, in file order, into the one (partial) pack.
    assert packs_seen == [[(0, 40), (40, 40)]]
    ptr_blob = next(b for b in put_bodies if b'"chunked-v2"' in b)
    refs = parse_pointer_v2(ptr_blob)
    assert [(r.chunk_digest, r.pack_offset) for r in refs] == [
        ("sha256:" + "a" * 64, 0),
        ("sha256:" + "a" * 64, 40),
    ]
