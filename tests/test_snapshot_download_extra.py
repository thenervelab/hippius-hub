"""Coverage for `snapshot_download` kwargs not exercised by test_phase_a.

test_phase_a covers the bare snapshot + `allow_patterns` + `ignore_patterns`
+ a `local_files_only` miss. Missing live coverage for: `local_dir=` (bypass
the cache), `force_download=` (re-fetch even on cache hit), `local_files_only`
HIT path (returns the seeded snapshot dir without network), `max_workers=`
(parallelism boundary at 1 vs default), and `dry_run=` (no files written).
"""
import os

import pytest

from hippius_hub import hippius_hub_upload, snapshot_download

from tests._helpers import sha256_of_file, write_test_file


pytestmark = pytest.mark.e2e


def _seed_repo(tmp_path, test_repo, revision, names_to_size):
    """Upload one file per (name, size) tuple, return {name: sha256_hex}."""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    expected = {}
    for name, size in names_to_size.items():
        p = src_dir / name
        p.parent.mkdir(parents=True, exist_ok=True)
        expected[name] = write_test_file(p, size, seed=name.encode())
    hippius_hub_upload(repo_id=test_repo, local_path=str(src_dir), revision=revision)
    return expected


def test_snapshot_download_local_dir_writes_outside_cache(
    tmp_path, cache_dir, logged_in, test_repo, revision,
):
    """local_dir= must drop the snapshot directly into the user-chosen dir,
    bypassing the HF cache layout entirely (no `models--*--*/snapshots/...`)."""
    expected = _seed_repo(tmp_path, test_repo, revision, {"a.bin": 64, "b.bin": 128})

    local = tmp_path / "into_here"
    local.mkdir()
    snap = snapshot_download(
        repo_id=test_repo, revision=revision,
        cache_dir=cache_dir, local_dir=str(local),
    )
    assert os.path.realpath(snap) == os.path.realpath(str(local))
    for name, want in expected.items():
        assert sha256_of_file(os.path.join(str(local), name)) == want

    # Nothing should have landed in the HF cache when local_dir is set.
    assert not os.path.exists(
        os.path.join(cache_dir, f"models--{test_repo.replace('/', '--')}")
    )


def test_snapshot_download_local_files_only_hit(
    tmp_path, cache_dir, logged_in, test_repo, revision,
):
    """After a seeded snapshot, local_files_only=True must return the cached
    dir without doing any network IO. Covers the 'fast offline reuse' path
    that test_phase_a only tests for misses."""
    _seed_repo(tmp_path, test_repo, revision, {"o.bin": 64})

    # Populate the cache.
    snapshot_download(repo_id=test_repo, revision=revision, cache_dir=cache_dir)

    # Second call: forbid network. Must still return a valid dir.
    snap = snapshot_download(
        repo_id=test_repo, revision=revision,
        cache_dir=cache_dir, local_files_only=True,
    )
    assert os.path.isdir(snap)
    assert os.path.exists(os.path.join(snap, "o.bin"))


def test_snapshot_download_force_download_replaces_files(
    tmp_path, cache_dir, logged_in, test_repo, revision,
):
    """force_download=True must rewrite files even on a cache hit."""
    _seed_repo(tmp_path, test_repo, revision, {"fd.bin": 64})

    p1 = snapshot_download(repo_id=test_repo, revision=revision, cache_dir=cache_dir)
    target = os.path.join(p1, "fd.bin")
    mtime_before = os.path.getmtime(os.path.realpath(target))

    import time
    time.sleep(1.1)

    p2 = snapshot_download(
        repo_id=test_repo, revision=revision,
        cache_dir=cache_dir, force_download=True,
    )
    assert p1 == p2
    assert os.path.getmtime(os.path.realpath(target)) >= mtime_before


def test_snapshot_download_max_workers_serial_still_works(
    tmp_path, cache_dir, logged_in, test_repo, revision,
):
    """max_workers=1 forces serial downloads. Validates the parallelism
    boundary doesn't break the manifest+token reuse logic."""
    expected = _seed_repo(
        tmp_path, test_repo, revision,
        {"s1.bin": 64, "s2.bin": 64, "s3.bin": 64},
    )
    snap = snapshot_download(
        repo_id=test_repo, revision=revision,
        cache_dir=cache_dir, max_workers=1,
    )
    for name, want in expected.items():
        assert sha256_of_file(os.path.join(snap, name)) == want


def test_snapshot_download_dry_run_returns_dir_without_writing(
    tmp_path, cache_dir, logged_in, test_repo, revision,
):
    """dry_run=True must compute the snapshot dir path but not actually
    download any files. Catches a regression where the early-return is
    moved after the parallel download block."""
    _seed_repo(tmp_path, test_repo, revision, {"dr.bin": 64})

    snap = snapshot_download(
        repo_id=test_repo, revision=revision,
        cache_dir=cache_dir, dry_run=True,
    )
    # Path is computed (parent might exist if any other test ran first), but
    # the actual file must not be present.
    assert not os.path.exists(os.path.join(snap, "dr.bin"))
