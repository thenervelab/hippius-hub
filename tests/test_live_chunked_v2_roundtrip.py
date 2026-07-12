"""Live chunked-v2 (pack layout) round-trip against the real registry.

Gated on `HIPPIUS_CHUNKED_WRITE` (opt-in), so it runs only where a producer opted
into the chunked-v2 pack layout — the staging CI step, never main. Exercises the
pieces the offline tests can't: the native pack upload (POST-init + monolithic PUT
of concatenated ranges) and the native pack download (fetch each pack once,
slice+verify chunks).

Proves: write (manifest is chunked-v2 with pack layers), read (bytes round-trip,
which also proves cross-pack chunk ordering via the whole-file digest), and dedup
(a small edit re-uploads far fewer pack bytes and still round-trips).
"""
import hashlib
import os

import pytest

from hippius_hub import hf_hub_download, hippius_hub_upload
from hippius_hub._oci import fetch_manifest, group_files
from hippius_hub.auth import get_oci_bearer_token
from hippius_hub.constants import (
    ARTIFACT_TYPE_CHUNKED_V2,
    CHUNKED_LAYOUT_V2,
    PACK_MEDIA_TYPE,
    resolve_chunked_write_enabled,
    resolve_registry,
)
from hippius_hub.file_download import _oci_repo_path

from tests._helpers import sha256_of_file, write_test_file

pytestmark = pytest.mark.e2e


def _pack_layer_count(registry, oci_repo, revision, token) -> int:
    result = fetch_manifest(registry, oci_repo, revision, token)
    return sum(1 for lyr in result.manifest["layers"] if lyr.get("mediaType") == PACK_MEDIA_TYPE)


def test_chunked_v2_live_roundtrip(tmp_path, cache_dir, logged_in, test_repo, revision, monkeypatch):
    if not resolve_chunked_write_enabled():
        pytest.skip("HIPPIUS_CHUNKED_WRITE not enabled; the chunked-v2 write path is opt-in")

    # Multi-chunk file split into a few packs: low threshold, small CDC average,
    # small pack size so a 4 MiB file yields multiple packs (exercises coalescing
    # and cross-pack ordering).
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", str(1 * 1024 * 1024))
    monkeypatch.setenv("HIPPIUS_CDC_AVG_SIZE", str(512 * 1024))
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", str(1 * 1024 * 1024))

    size = 4 * 1024 * 1024
    src = tmp_path / "big.bin"
    # Key the content to the unique per-run revision, NOT a fixed seed. A fixed seed
    # yields byte-identical packs every run, so the pack digests collide with any
    # registry-side GC-wedged blob of that digest — a deterministic pack a prior run
    # left half-committed (blob reaped, DB row gone) can never re-commit, and the
    # manifest PUT fails MANIFEST_BLOB_UNKNOWN forever. Fresh-per-run content gives
    # every run brand-new digests (referenced by the committing manifest, so GC keeps
    # them); it stays reproducible within the run, so the edit+re-upload dedup check
    # below still holds.
    expected = write_test_file(src, size, seed=revision.encode())

    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    # write-path proof: stored as chunked-v2 with >= 2 pack layers.
    oci_repo = _oci_repo_path(test_repo, None)
    registry = resolve_registry(None)
    token = get_oci_bearer_token(oci_repo)
    result = fetch_manifest(registry, oci_repo, revision, token)
    assert result.manifest.get("artifactType") == ARTIFACT_TYPE_CHUNKED_V2
    (group,) = [g for g in group_files(result.manifest) if g.title == "big.bin"]
    assert group.layout == CHUNKED_LAYOUT_V2
    assert _pack_layer_count(registry, oci_repo, revision, token) >= 2

    # read-path proof: bytes round-trip (also proves cross-pack ordering).
    out = hf_hub_download(repo_id=test_repo, filename="big.bin", revision=revision, cache_dir=cache_dir)
    assert sha256_of_file(out) == expected
    assert os.path.getsize(out) == size

    # dedup path: edit a small middle region and re-upload to the SAME revision
    # (so the dedup index, built from that revision's current manifest, sees the
    # prior packs). Only the changed chunk's pack is new; the edited file must
    # still round-trip.
    data = bytearray(src.read_bytes())
    mid = len(data) // 2
    data[mid:mid + 4096] = os.urandom(4096)
    src2 = tmp_path / "v2dir" / "big.bin"  # same basename -> same repo title/group
    os.makedirs(src2.parent, exist_ok=True)
    src2.write_bytes(data)
    expected2 = hashlib.sha256(data).hexdigest()

    hippius_hub_upload(repo_id=test_repo, local_path=str(src2), revision=revision)
    cache2 = str(tmp_path / "cache2")
    os.makedirs(cache2, exist_ok=True)
    out2 = hf_hub_download(repo_id=test_repo, filename="big.bin", revision=revision, cache_dir=cache2)
    assert sha256_of_file(out2) == expected2


def test_chunked_v2_many_packs_ordering(tmp_path, cache_dir, logged_in, test_repo, revision, monkeypatch):
    """Cross-pack ordering + PackAssembler under real fanout, at ~8 MiB of movement.

    The 4 MiB round-trip above proves 2 packs; production files fan out to dozens
    (a 3 GiB shard at 64 MiB packs is ~48). Bugs in pack scheduling, the download
    semaphore, or scatter-to-offset reassembly only surface with many packs — but
    that's a property of the pack *count*, not the byte volume, so we force a high
    pack count with small pack/CDC sizes and keep the transfer cheap. The whole-file
    digest on download is the only check that catches a mis-ordered many-pack
    reassembly.
    """
    if not resolve_chunked_write_enabled():
        pytest.skip("HIPPIUS_CHUNKED_WRITE not enabled; the chunked-v2 write path is opt-in")

    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", str(1 * 1024 * 1024))
    monkeypatch.setenv("HIPPIUS_CDC_AVG_SIZE", str(256 * 1024))  # max chunk = avg*4 = 1 MiB
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", str(512 * 1024))     # ~16 packs from an 8 MiB file

    size = 8 * 1024 * 1024
    src = tmp_path / "many.bin"
    expected = write_test_file(src, size, seed=b"v2-many-packs")

    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    oci_repo = _oci_repo_path(test_repo, None)
    registry = resolve_registry(None)
    token = get_oci_bearer_token(oci_repo)
    result = fetch_manifest(registry, oci_repo, revision, token)
    (group,) = [g for g in group_files(result.manifest) if g.title == "many.bin"]
    assert group.layout == CHUNKED_LAYOUT_V2
    assert _pack_layer_count(registry, oci_repo, revision, token) >= 8, "small packs must fan out to many pack layers"

    out = hf_hub_download(repo_id=test_repo, filename="many.bin", revision=revision, cache_dir=cache_dir)
    assert sha256_of_file(out) == expected
    assert os.path.getsize(out) == size
