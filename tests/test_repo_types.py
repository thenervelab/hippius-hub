"""End-to-end coverage of repo_type=model / dataset / space.

Each test runs against the corresponding Harbor project (`test` / `datasets` /
`spaces`) via the system-level robot. Validates that the OCI-path mapping
and cache-dirname mapping translate to a working full round-trip — not just
the unit-tested string mapping.
"""
import os
import uuid

import pytest

from hippius_hub import (
    file_exists,
    hf_hub_download,
    list_repo_files,
    repo_exists,
    revision_exists,
    upload_file,
)
from hippius_hub.file_download import _cache_dirname

from tests._helpers import sha256_of_file, write_test_file


pytestmark = pytest.mark.e2e


# (repo_type, default repo_id for that type). The model case keeps the existing
# test/e2e-client so we re-use the seeded project; dataset/space use the
# corresponding projects pre-created on the registry.
_REPO_TYPE_CASES = [
    ("model", "test/e2e-client"),
    ("dataset", "e2e/client"),
    ("space", "e2e/client"),
]


@pytest.fixture
def per_type_repo(request, logged_in):
    repo_type, repo_id = request.param
    return {"repo_type": repo_type, "repo_id": repo_id}


@pytest.mark.parametrize("per_type_repo", _REPO_TYPE_CASES, indirect=True, ids=lambda c: c[0])
def test_roundtrip_per_repo_type(per_type_repo, tmp_path, cache_dir, revision):
    """upload_file → hf_hub_download produces a byte-identical file for each
    repo_type, with the cache landing under the right HF-style directory."""
    repo_type = per_type_repo["repo_type"]
    repo_id = per_type_repo["repo_id"]

    src = tmp_path / "rt.bin"
    expected = write_test_file(src, 1024, seed=repo_type.encode())

    upload_file(
        path_or_fileobj=str(src),
        path_in_repo="rt.bin",
        repo_id=repo_id,
        repo_type=repo_type,
        revision=revision,
    )

    out = hf_hub_download(
        repo_id=repo_id,
        filename="rt.bin",
        repo_type=repo_type,
        revision=revision,
        cache_dir=cache_dir,
    )
    assert sha256_of_file(out) == expected

    # Cache lives under the type-appropriate HF dirname (models--, datasets--, spaces--).
    expected_dirname = _cache_dirname(repo_id, repo_type)
    assert expected_dirname in out


@pytest.mark.parametrize("per_type_repo", _REPO_TYPE_CASES, indirect=True, ids=lambda c: c[0])
def test_inspection_helpers_per_repo_type(per_type_repo, tmp_path, revision):
    """After uploading one file, list/exists helpers see it for every type."""
    repo_type = per_type_repo["repo_type"]
    repo_id = per_type_repo["repo_id"]

    src = tmp_path / "insp.bin"
    write_test_file(src, 64, seed=f"insp-{repo_type}".encode())

    upload_file(
        path_or_fileobj=str(src),
        path_in_repo="insp.bin",
        repo_id=repo_id,
        repo_type=repo_type,
        revision=revision,
    )

    assert repo_exists(repo_id, repo_type=repo_type) is True
    assert revision_exists(repo_id, revision, repo_type=repo_type) is True
    assert file_exists(repo_id, "insp.bin", repo_type=repo_type, revision=revision) is True
    assert file_exists(repo_id, "no-such-thing.bin", repo_type=repo_type, revision=revision) is False

    files = list_repo_files(repo_id, repo_type=repo_type, revision=revision)
    assert "insp.bin" in files
