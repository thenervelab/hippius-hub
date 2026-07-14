"""C2 — deduplicate chunks WITHIN a single folder upload (offline).

`upload_folder` builds its dedup index once from the prior revision and shares one
STATIC copy across the per-file thread pool — it never updates it as files
complete. So two files in the SAME push that contain the same chunk each pack and
upload it. This pins the fix: a chunk packed by the first file is reused by the
second, not re-uploaded.

The native chunker/packer are stubbed, so this is a pure orchestration check.
`packs_seen` records every `(offset, len)` range handed to the packer, so the
count of chunks actually packed across the whole push is the observable.
"""
from __future__ import annotations

import hashlib
import os

import httpx
import respx

from hippius_hub import file_upload
from hippius_hub.file_upload import upload_folder
from tests.respx_fixtures import MOCK_REGISTRY, token_route

REPO = "acme/model"

# Chunk digests (bare hex, exactly as chunk_and_hash_native returns them). X is
# shared by both files; Y is A-only, Z is B-only.
HX, HY, HZ = "a" * 64, "b" * 64, "d" * 64

# Per basename: (whole_file_hex, [(chunk_hex, offset, size), ...]) — the exact shape
# chunk_and_hash_native returns. Both files begin with the shared chunk X.
_FILES = {
    "a.bin": ("1" * 64, [(HX, 0, 40), (HY, 40, 60)]),
    "b.bin": ("2" * 64, [(HX, 0, 40), (HZ, 40, 50)]),
}


def _wire(monkeypatch):
    """respx-mock the registry and stub the two natives; return `packs_seen`."""
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_REGISTRY_URL", MOCK_REGISTRY)

    monkeypatch.setattr(
        file_upload, "chunk_and_hash_native", lambda path, avg: _FILES[os.path.basename(path)]
    )

    packs_seen = []

    def _fake_pack(uploads_url, path, ranges, auth_token):
        ranges = list(ranges)
        packs_seen.append(ranges)
        # Deterministic content-addressed stand-in; the mock never reads bytes.
        return hashlib.sha256(repr(ranges).encode()).hexdigest()

    monkeypatch.setattr(file_upload, "pack_upload_native", _fake_pack)

    token_route(respx.mock)
    respx.head(url__regex=rf"{MOCK_REGISTRY}/v2/{REPO}/blobs/.*").mock(return_value=httpx.Response(404))
    respx.post(f"{MOCK_REGISTRY}/v2/{REPO}/blobs/uploads/").mock(
        return_value=httpx.Response(202, headers={"Location": f"{MOCK_REGISTRY}/v2/{REPO}/blobs/uploads/uuid"})
    )
    respx.put(url__startswith=f"{MOCK_REGISTRY}/v2/{REPO}/blobs/uploads/uuid").mock(
        return_value=httpx.Response(201)
    )
    respx.get(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(return_value=httpx.Response(404))
    respx.put(f"{MOCK_REGISTRY}/v2/{REPO}/manifests/main").mock(
        return_value=httpx.Response(201, headers={"Docker-Content-Digest": "sha256:" + "f" * 64})
    )
    return packs_seen


@respx.mock
def test_folder_upload_dedups_a_chunk_shared_between_two_files(monkeypatch, tmp_path):
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "1")   # chunk both files
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "1")
    monkeypatch.setenv("HIPPIUS_CHUNKED_LAYOUT", "v2")
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", "100000")    # each file's chunks fit one pack

    packs_seen = _wire(monkeypatch)

    (tmp_path / "a.bin").write_bytes(b"A" * 100)
    (tmp_path / "b.bin").write_bytes(b"B" * 90)

    # max_workers=1 serialises the two files, so the second is guaranteed to see the
    # first's published chunks — deterministic full dedup, no race. (The fix is also
    # correct under concurrency; the plan just accepts a racing double-pack there.)
    upload_folder(repo_id=REPO, folder_path=str(tmp_path), revision="main", token="t", max_workers=1)

    total_packed = sum(len(ranges) for ranges in packs_seen)
    # Three DISTINCT chunks (X, Y, Z) across the two files. Without intra-batch dedup
    # the shared X is packed by BOTH files → 4. With it, X is packed once → 3.
    assert total_packed == 3, (
        f"expected 3 chunks packed (X deduped within the batch), got {total_packed}; packs={packs_seen}"
    )
