"""Live test for audit M1 / IMP-4: snapshot_download(dry_run=True) is a true
I/O short-circuit.

The localhost test `test_snapshot_download_dry_run.py` pins this via
respx in strict mode (zero registered routes → any HTTP attempt fails).
This file is the live counterpart: against the real Hippius registry,
snapshot_download(dry_run=True) must return the snapshot directory
without materializing any of the blobs that an actual download would
produce.

Behavioral pin: after the dry call, the returned directory either
doesn't exist on disk OR exists but is empty. Without the M1 fix, the
function would still resolve the manifest (network) and silently
materialize the cache layout under blobs/ even though no real download
happened.
"""
from __future__ import annotations

import os

import pytest

from hippius_hub import hippius_hub_upload, snapshot_download

from tests._helpers import write_test_file


pytestmark = pytest.mark.e2e


def test_snapshot_dry_run_materializes_no_files(
    tmp_path, cache_dir, logged_in, test_repo, revision,
):
    """Upload a file, then snapshot_download(dry_run=True) and assert
    nothing landed on disk.

    The upload step is needed so the revision actually exists — if we
    dry-ran against a never-uploaded revision, a regression that DID
    hit the manifest endpoint would surface as RevisionNotFoundError,
    not a silent download. The upload step also protects against false
    negatives from caching effects.
    """
    src = tmp_path / "snapshot-target.bin"
    write_test_file(src, 4096, seed=b"snap-dry-run")
    hippius_hub_upload(
        repo_id=test_repo,
        local_path=str(src),
        revision=revision,
    )

    # Fresh cache dir for the dry run so any pre-existing files from
    # other tests can't masquerade as "nothing was downloaded".
    dry_cache = os.path.join(cache_dir, "dry-run-cache")
    os.makedirs(dry_cache, exist_ok=True)

    snapshot_dir = snapshot_download(
        repo_id=test_repo,
        revision=revision,
        cache_dir=dry_cache,
        dry_run=True,
    )

    # The returned path is the expected layout location.
    assert "models--" in snapshot_dir and revision in snapshot_dir, (
        f"dry_run must still return the expected snapshot path; got "
        f"{snapshot_dir!r}"
    )

    # The directory must NOT have any downloaded files. Two acceptable
    # states: directory absent, or directory exists but empty.
    if os.path.exists(snapshot_dir):
        contents = os.listdir(snapshot_dir)
        assert contents == [], (
            f"dry_run=True must materialize no files; found "
            f"{contents!r} in {snapshot_dir!r}"
        )

    # And blobs/ — the content-addressed cache layout — must be absent
    # too. A regression that did the manifest fetch but skipped blob
    # downloads would still create the blobs/ subdirectory (mkdir
    # happens in _download_to_cache, BEFORE the network call).
    blobs_marker = os.path.join(
        dry_cache,
        snapshot_dir.split(dry_cache + os.sep, 1)[-1].split(os.sep)[0],
        "blobs",
    )
    assert not os.path.exists(blobs_marker), (
        f"dry_run=True leaked the blobs/ cache directory at "
        f"{blobs_marker!r} — that means _download_to_cache was called, "
        f"which means the dry_run short-circuit fired too late."
    )
