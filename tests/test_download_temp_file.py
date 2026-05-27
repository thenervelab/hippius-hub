"""Behavioral test for unique temp-file naming in _download_to_cache.

The previous version of this file was a source-grep — `assert
"tempfile.mkstemp" in inspect.getsource(...)`. That passes even under a
regression that switches back to a non-unique pattern like
`f"tmp_{filename}_{os.getpid()}"`, which produces collisions when two
processes with the same pid (after a fork) write to the same blobs_dir.

Now we drive `_download_to_cache` directly with `download_file_native`
mocked out, capture each `temp_path` it was asked to write to, and
assert the paths are pairwise distinct. Twenty calls with identical
inputs would catch any regression that drops the per-call entropy —
mkstemp's `os.urandom`-seeded suffix.
"""
from __future__ import annotations

import os
import threading

import pytest

from hippius_hub import file_download


@pytest.fixture
def cache_layout(tmp_path):
    """A repo_dir / blobs_dir / snapshots_dir trio for _download_to_cache."""
    repo_dir = tmp_path / "repo"
    snapshots_dir = tmp_path / "snapshots" / "main"
    repo_dir.mkdir()
    snapshots_dir.mkdir(parents=True)
    return repo_dir, snapshots_dir


def test_temp_paths_are_unique_per_call(cache_layout, monkeypatch):
    """20 sequential downloads of the SAME filename must produce 20 distinct
    temp paths. A regression that drops mkstemp's per-call entropy would
    collide on call 2.
    """
    repo_dir, snapshots_dir = cache_layout
    seen_temp_paths: list[str] = []

    def fake_download(*, url, dest_path, auth_token, chunk_size, verify_hash):
        seen_temp_paths.append(dest_path)
        with open(dest_path, "wb") as f:
            f.write(b"x")
        return None  # signals skipped-verify, falls back to manifest digest

    monkeypatch.setattr(file_download, "download_file_native", fake_download)

    for _ in range(20):
        file_download._download_to_cache(
            blob_url="https://registry.test/v2/foo/bar/blobs/sha256:" + "a" * 64,
            repo_dir=str(repo_dir),
            snapshots_dir=str(snapshots_dir),
            filename="model.safetensors",
            oci_token="literal-token",
            target_digest="sha256:" + "a" * 64,
        )

    assert len(seen_temp_paths) == 20
    assert len(set(seen_temp_paths)) == 20, (
        f"temp paths collided after 20 sequential calls: "
        f"{sorted(set(seen_temp_paths))!r}"
    )


def test_temp_paths_unique_under_thread_concurrency(cache_layout, monkeypatch):
    """Eight threads racing on _download_to_cache for the same filename must
    get distinct temp paths. The OS-level guarantee from mkstemp is what
    actually closes the race; this test pins that we're using it correctly.

    `_create_symlink` is also mocked out — it has its own TOCTOU race on
    `os.path.exists(dst) + os.remove(dst)` that fires when threads converge
    on the same blob_path. That race is orthogonal to the mkstemp uniqueness
    claim this test makes; stubbing the symlink step keeps the assertion
    surface focused.
    """
    repo_dir, snapshots_dir = cache_layout
    seen_temp_paths: list[str] = []
    seen_lock = threading.Lock()
    barrier = threading.Barrier(8)

    def fake_download(*, url, dest_path, auth_token, chunk_size, verify_hash):
        # Force all 8 threads into mkstemp at roughly the same time. Without
        # the barrier, Python's scheduling can serialize them and a
        # non-unique implementation would still happen to pass.
        barrier.wait(timeout=5)
        with seen_lock:
            seen_temp_paths.append(dest_path)
        with open(dest_path, "wb") as f:
            f.write(b"x")
        return None

    monkeypatch.setattr(file_download, "download_file_native", fake_download)
    # The symlink step races on a shared dst when threads converge — see
    # docstring above. Replace it with a no-op so the test only assesses
    # what it claims to.
    monkeypatch.setattr(file_download, "_create_symlink", lambda src, dst: None)

    def worker():
        file_download._download_to_cache(
            blob_url="https://registry.test/v2/foo/bar/blobs/sha256:" + "b" * 64,
            repo_dir=str(repo_dir),
            snapshots_dir=str(snapshots_dir),
            filename="weights.bin",
            oci_token="literal-token",
            target_digest="sha256:" + "b" * 64,
        )

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert len(seen_temp_paths) == 8
    assert len(set(seen_temp_paths)) == 8, (
        f"concurrent temp paths collided: {sorted(set(seen_temp_paths))!r}"
    )


def test_temp_file_cleaned_up_on_download_failure(cache_layout, monkeypatch):
    """If download_file_native raises, the mkstemp file must be removed
    before the exception bubbles. Otherwise repeated retries leak inodes
    in blobs_dir until disk-full.
    """
    repo_dir, snapshots_dir = cache_layout
    captured: dict[str, str] = {}

    def fake_download(*, url, dest_path, auth_token, chunk_size, verify_hash):
        captured["temp_path"] = dest_path
        # mkstemp already created the empty file; simulate a partial write
        # then a network error.
        with open(dest_path, "wb") as f:
            f.write(b"partial")
        raise RuntimeError("simulated network drop")

    monkeypatch.setattr(file_download, "download_file_native", fake_download)

    with pytest.raises(RuntimeError, match="simulated network drop"):
        file_download._download_to_cache(
            blob_url="https://registry.test/v2/foo/bar/blobs/sha256:" + "c" * 64,
            repo_dir=str(repo_dir),
            snapshots_dir=str(snapshots_dir),
            filename="orphan.bin",
            oci_token="literal-token",
            target_digest="sha256:" + "c" * 64,
        )

    assert not os.path.exists(captured["temp_path"]), (
        f"orphan temp file {captured['temp_path']!r} still on disk after "
        "download error — would leak across retries"
    )
