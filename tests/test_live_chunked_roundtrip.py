"""Live end-to-end proof of the chunked-artifact layout against the real registry.

Gated on `HIPPIUS_CHUNKED_WRITE` being enabled. That gate is off by default this
release (the reader-side layout guard ships in the same release, so no already-
deployed reader carries it), so this test runs only where a producer has opted in
— the staging CI step (`.github/workflows/e2e.yml`), never on `main` or a PR.

Enabling the gate both turns the feature on AND un-skips this test, so one env var
drives both. Everything here targets the dedicated `test/e2e-client` namespace and
uploads ordinary content-addressed blobs — it changes nothing in production's
config or deployment (org-wide trace: chunking is a self-contained client change).

The three assertions cover the full path a plain-blob round-trip cannot:
- **write**: the stored manifest is the chunked layout (artifactType + K>=2 chunk
  layers), proving the uploader emitted chunks rather than one blob;
- **read**: the bytes round-trip, which also proves chunk *ordering* — the client's
  whole-file `sha256(concat)` check is the only thing that catches a mis-ordered
  reassembly;
- **dedup**: an identical re-upload to a fresh revision (every chunk `HEAD`-hits and
  is skipped) still assembles a correct manifest and round-trips.
"""
import os

import pytest

from hippius_hub import hf_hub_download, hippius_hub_upload
from hippius_hub._oci import fetch_manifest, group_files
from hippius_hub.auth import get_oci_bearer_token
from hippius_hub.constants import (
    ARTIFACT_TYPE_CHUNKED,
    resolve_chunked_write_enabled,
    resolve_registry,
)
from hippius_hub.file_download import _oci_repo_path

from tests._helpers import sha256_of_file, write_test_file


pytestmark = pytest.mark.e2e


def test_chunked_layout_live_roundtrip(tmp_path, cache_dir, logged_in, test_repo, revision, monkeypatch):
    if not resolve_chunked_write_enabled():
        pytest.skip("HIPPIUS_CHUNKED_WRITE not enabled; the chunked write path is opt-in this release")

    # Force a multi-chunk split on a CI-sized file without moving hundreds of MiB:
    # a low threshold sends a small file down the chunked path, and CDC avg 512 KiB
    # caps FastCDC's max chunk at avg*4 = 2 MiB, so a 4 MiB file is guaranteed >= 2
    # chunks — enough to exercise ordering and the multi-blob assemble.
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", str(1 * 1024 * 1024))
    monkeypatch.setenv("HIPPIUS_CDC_AVG_SIZE", str(512 * 1024))

    size = 4 * 1024 * 1024
    src = tmp_path / "big.bin"
    expected = write_test_file(src, size, seed=b"chunked-live")

    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    # write-path proof: the real registry stored the chunked layout, not a plain blob.
    oci_repo = _oci_repo_path(test_repo, None)
    oci_token = get_oci_bearer_token(oci_repo)
    result = fetch_manifest(resolve_registry(None), oci_repo, revision, oci_token)
    assert result is not None
    assert result.manifest.get("artifactType") == ARTIFACT_TYPE_CHUNKED
    (group,) = [g for g in group_files(result.manifest) if g.title == "big.bin"]
    assert group.is_chunked and len(group.chunks) >= 2

    # read-path proof: bytes round-trip (the whole-file digest also proves chunk order).
    out = hf_hub_download(repo_id=test_repo, filename="big.bin", revision=revision, cache_dir=cache_dir)
    assert sha256_of_file(out) == expected
    assert os.path.getsize(out) == size

    # dedup path: identical re-upload to a fresh revision — every chunk HEAD-hits and
    # is skipped — must still assemble a correct manifest and round-trip. A separate
    # cache dir forces a real fetch instead of a content-addressed cache hit.
    revision2 = f"{revision}-again"
    cache_dir2 = str(tmp_path / "cache2")
    os.makedirs(cache_dir2, exist_ok=True)
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision2)
    out2 = hf_hub_download(repo_id=test_repo, filename="big.bin", revision=revision2, cache_dir=cache_dir2)
    assert sha256_of_file(out2) == expected
