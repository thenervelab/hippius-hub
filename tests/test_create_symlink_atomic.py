"""Concurrent _create_symlink test (audit RACE-2).

The previous implementation used:

    if os.path.exists(dst):
        os.remove(dst)
    os.symlink(rel_src, dst)

…which has two TOCTOU windows:

  1. Between `exists(dst) → True` and `os.remove(dst)`: another thread
     can remove the same path, so our `os.remove` raises
     FileNotFoundError.
  2. Between `exists(dst) → False` and `os.symlink(dst)`: another
     thread can create the symlink first, so ours raises
     FileExistsError.

This surfaced as ResourceWarnings in `test_download_temp_file.py`'s
concurrent test (which is why we used to mock _create_symlink out
there). The fix uses the atomic-rename pattern: build the new
symlink at a per-process-unique sibling path, then os.replace into
the final destination.

This file pins the fix at the function level:
  - 16 threads converge on the same `dst` from different `src` blobs
  - Every thread must complete without raising
  - The final symlink must exist and resolve to a valid blob
"""
from __future__ import annotations

import os
import sys
import threading

import pytest

from hippius_hub import file_download


pytestmark = pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="Windows symlink permissions require developer mode; the race fix "
           "applies cross-platform but the test is hard to make reliable on Windows.",
)


def test_concurrent_symlink_no_raise(tmp_path):
    """Sixteen threads concurrently create a symlink at the same `dst`,
    each pointing at a distinct blob. The pre-fix implementation would
    raise FileNotFoundError or FileExistsError on at least one thread.
    """
    blobs_dir = tmp_path / "blobs"
    snapshots_dir = tmp_path / "snapshots"
    blobs_dir.mkdir()
    snapshots_dir.mkdir()

    # 16 distinct blobs — every thread targets a different src so we can
    # verify the FINAL symlink resolves to ONE of them (last write wins,
    # which is expected for replace-based concurrency).
    blob_paths = []
    for i in range(16):
        blob = blobs_dir / f"sha256:blob-{i}"
        blob.write_bytes(f"contents-{i}".encode())
        blob_paths.append(str(blob))

    dst = str(snapshots_dir / "shared.bin")
    barrier = threading.Barrier(16)
    errors: list[BaseException] = []

    def worker(src: str):
        barrier.wait(timeout=5)
        try:
            file_download._create_symlink(src, dst)
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(p,)) for p in blob_paths]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert errors == [], (
        f"_create_symlink raised on at least one thread under concurrent "
        f"writes: {errors!r}. The atomic-rename pattern should prevent any "
        f"FileExistsError / FileNotFoundError."
    )

    # The final dst must be a symlink (or hardlink/copy under fallback)
    # pointing at one of the 16 blobs. We don't pin WHICH one — last
    # write wins is the documented contract.
    assert os.path.exists(dst), "final symlink missing after 16 concurrent writers"
    # Resolve and confirm it lands on a real blob.
    if os.path.islink(dst):
        target = os.path.realpath(dst)
        assert target in blob_paths, (
            f"symlink target {target!r} not in blob set "
            f"({sorted(blob_paths)!r})"
        )


def test_no_temp_files_leak_after_success(tmp_path):
    """After a normal call, no `.token-*.tmp` / `.<sha>.*.tmp` litter is
    left in the snapshot directory. Pin that the per-call temp name
    used for the atomic rename is consumed by `os.replace` and not
    left behind.
    """
    blobs_dir = tmp_path / "blobs"
    snapshots_dir = tmp_path / "snapshots"
    blobs_dir.mkdir()
    snapshots_dir.mkdir()

    blob = blobs_dir / "sha256:test"
    blob.write_bytes(b"x")
    dst = str(snapshots_dir / "out.bin")

    file_download._create_symlink(str(blob), dst)

    leftover = [p.name for p in snapshots_dir.iterdir() if p.name != "out.bin"]
    assert leftover == [], (
        f"unexpected files left in snapshots dir: {leftover!r}. "
        f"The .tmp sibling used for atomic-rename should be consumed."
    )


def test_symlink_overwrite_existing_target(tmp_path):
    """When dst already exists (e.g. a re-download replacing a stale
    symlink), the atomic-rename pattern must replace it cleanly. The
    pre-fix `if exists: remove` path raced; the new code lets
    `os.replace` handle the overwrite as a single atomic syscall.
    """
    blobs_dir = tmp_path / "blobs"
    snapshots_dir = tmp_path / "snapshots"
    blobs_dir.mkdir()
    snapshots_dir.mkdir()

    blob_old = blobs_dir / "sha256:old"
    blob_new = blobs_dir / "sha256:new"
    blob_old.write_bytes(b"old")
    blob_new.write_bytes(b"new")
    dst = str(snapshots_dir / "out.bin")

    file_download._create_symlink(str(blob_old), dst)
    assert os.path.realpath(dst) == str(blob_old.resolve())

    file_download._create_symlink(str(blob_new), dst)
    assert os.path.realpath(dst) == str(blob_new.resolve())
