import os
from pathlib import Path

import pytest

from tests._helpers import write_test_file


pytestmark = pytest.mark.e2e


HF_REFERENCE_REPO = "hf-internal-testing/tiny-random-gpt2"
HF_REFERENCE_FILE = "config.json"


def _assert_hf_compatible_layout(returned_path, cache_dir, filename):
    """Assert that `returned_path` follows the HF Hub cache schema:
        cache_dir/models--*--*/snapshots/<rev>/<filename>
    resolving to:
        cache_dir/models--*--*/blobs/<digest>
    """
    cd = Path(cache_dir).resolve()
    parts = list(Path(returned_path).relative_to(cd).parts)

    repo_idx = next((i for i, p in enumerate(parts) if p.startswith("models--")), None)
    assert repo_idx is not None, f"no models--*--* dir in {parts}"

    repo_dir = cd.joinpath(*parts[: repo_idx + 1])
    assert (repo_dir / "blobs").is_dir(), f"missing {repo_dir}/blobs"
    assert (repo_dir / "snapshots").is_dir(), f"missing {repo_dir}/snapshots"

    assert parts[repo_idx + 1] == "snapshots"
    assert parts[-1] == os.path.basename(filename)

    assert Path(returned_path).is_file(), f"{returned_path} is not a regular file"

    real = Path(os.path.realpath(returned_path))
    assert (repo_dir / "blobs").resolve() in real.parents, (
        f"realpath {real} is not under {repo_dir}/blobs"
    )


def test_hf_cache_layout_reference(tmp_path):
    """Reference oracle — pin our assumptions about huggingface_hub's cache schema.
    If this fails, HF changed their layout and our parity assertions need updating."""
    from huggingface_hub import hf_hub_download as real_hf_download

    cache = tmp_path / "hf_cache"
    cache.mkdir()

    out = real_hf_download(
        repo_id=HF_REFERENCE_REPO,
        filename=HF_REFERENCE_FILE,
        cache_dir=str(cache),
    )
    _assert_hf_compatible_layout(out, str(cache), HF_REFERENCE_FILE)


@pytest.mark.e2e
def test_hippius_cache_layout_matches_hf(tmp_path, logged_in, test_repo, revision):
    """Apply the exact same shape assertions to hippius_hub's output."""
    from hippius_hub import hippius_hub_upload, hf_hub_download

    src = tmp_path / "parity.bin"
    write_test_file(src, 256, seed=b"parity")
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    cache = tmp_path / "hippius_cache"
    cache.mkdir()

    out = hf_hub_download(
        repo_id=test_repo,
        filename="parity.bin",
        revision=revision,
        cache_dir=str(cache),
    )
    _assert_hf_compatible_layout(out, str(cache), "parity.bin")


@pytest.mark.e2e
def test_returned_path_is_inside_snapshots(tmp_path, logged_in, test_repo, revision):
    from hippius_hub import hippius_hub_upload, hf_hub_download

    src = tmp_path / "shape.bin"
    write_test_file(src, 128, seed=b"shape")
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    cache = tmp_path / "cache"
    cache.mkdir()

    out = hf_hub_download(
        repo_id=test_repo,
        filename="shape.bin",
        revision=revision,
        cache_dir=str(cache),
    )
    rel = Path(out).relative_to(cache)
    assert rel.parts[1] == "snapshots"
    assert rel.parts[2] == revision
    assert rel.parts[-1] == "shape.bin"


@pytest.mark.e2e
def test_symlink_target_resolves_to_blob(tmp_path, logged_in, test_repo, revision):
    from hippius_hub import hippius_hub_upload, hf_hub_download

    src = tmp_path / "link.bin"
    write_test_file(src, 64, seed=b"link")
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    cache = tmp_path / "cache"
    cache.mkdir()

    out = hf_hub_download(
        repo_id=test_repo,
        filename="link.bin",
        revision=revision,
        cache_dir=str(cache),
    )
    real = Path(os.path.realpath(out))
    assert "blobs" in real.parts
    assert real.name.startswith("sha256:")
