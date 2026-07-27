"""Hourly production smoke suite for the published hippius_hub client.

Answers one question every hour: can a user who just ran `pip install
hippius_hub` push a 100 MiB model to registry.hippius.com and pull it back
byte-for-byte? Everything else here exists to make a failure legible.

Tests are numbered because they share session state and run in file order:
the sweep runs first (so a wedged namespace doesn't grow unbounded), then the
client sanity checks, then upload, then download.
"""
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import _revisions
import hippius_hub
from hippius_hub import console


def _sha256_of_file(path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def test_01_cleanup_old_revisions(
    logged_in, console_logged_in, smoke_repo, qualified_repo, retention_cutoff
):
    """Delete `smoke-*` revisions older than the retention window.

    Each run pushes ~100 MiB of fresh, non-dedupable bytes; without this the
    namespace grows ~2.4 GB/day. Only revisions whose name parses as one of
    ours are touched, so e2e revisions in the same repo are never at risk.
    """
    try:
        refs = hippius_hub.list_repo_refs(smoke_repo)
    except hippius_hub.RepositoryNotFoundError:
        # First run against a brand-new namespace: `upload_folder` creates the
        # repo, so there is genuinely nothing to sweep yet. Skipping (rather
        # than failing) keeps the very first run from going red, and still
        # shows up in the report if the repo later disappears for real.
        pytest.skip(f"{smoke_repo} does not exist yet — nothing to sweep")

    candidates = [r.name for r in refs.tags] + [b.name for b in refs.branches]

    deleted = 0
    for revision in candidates:
        stamp = _revisions.parse_timestamp(revision)
        if stamp is None or stamp >= retention_cutoff:
            continue
        try:
            console.delete_artifact(qualified_repo, revision)
        except console.ConsoleError as e:
            # A 404 means the model indexer never saw the tag, or a concurrent
            # run already reaped it. Both are benign; anything else is a real
            # regression in the delete path and must fail the suite.
            if e.status_code != 404:
                raise
            continue
        deleted += 1

    print(
        f"Cleanup: deleted {deleted} smoke revisions older than "
        f"{retention_cutoff.isoformat()} from {smoke_repo}"
    )


def test_02_published_client_loads(client_path):
    """The wheel under test is the one from PyPI, and its Rust extension
    imports. A broken abi3 wheel (wrong tag, missing .so) fails right here
    instead of surfacing as a confusing upload error three tests later."""
    from hippius_hub import hippius_core

    assert hippius_core is not None
    print(f"Testing hippius_hub {hippius_hub.__version__} from {client_path}")


def test_03_cli_entrypoint(client_path):
    """`hippius-hub --version` runs. The console script is declared in
    pyproject; a packaging change that drops it breaks every user's install
    while the Python API keeps working, so it needs its own check.

    Resolved next to `sys.executable` rather than via PATH: that's where pip
    puts the script for *this* interpreter, and CI invokes `.venv/bin/pytest`
    without activating the venv, so PATH would not contain it.
    """
    candidate = Path(sys.executable).parent / "hippius-hub"
    executable = str(candidate) if candidate.exists() else shutil.which("hippius-hub")
    assert executable, (
        f"No `hippius-hub` console script next to {sys.executable} or on PATH — "
        f"the entrypoint is missing from the published wheel."
    )

    result = subprocess.run([executable, "--version"], capture_output=True, text=True)
    assert result.returncode == 0, (
        f"`hippius-hub --version` exited {result.returncode} — the CLI "
        f"entrypoint is broken in the published wheel.\n{result.stderr}"
    )
    assert hippius_hub.__version__ in result.stdout, (
        f"CLI reported {result.stdout.strip()!r} but the package version is "
        f"{hippius_hub.__version__!r} — version reporting has drifted."
    )


def test_04_whoami(logged_in):
    """Credentials are accepted by production. Isolating auth from the upload
    means an expired CI robot secret reads as 'auth failed', not '100 MiB
    upload failed'."""
    identity = hippius_hub.whoami()

    assert identity, "whoami() returned an empty response — authentication against production failed."
    assert identity.get("name"), (
        f"whoami() returned no username: {identity!r} — the registry accepted "
        f"the request but did not identify the caller."
    )
    print(f"Authenticated as {identity['name']}")


def test_05_upload_model(logged_in, smoke_repo, session_revision, model_dir):
    """Push the 100 MiB model folder at a fresh revision."""
    root, _ = model_dir

    commit = hippius_hub.upload_folder(
        repo_id=smoke_repo,
        folder_path=str(root),
        revision=session_revision,
    )

    assert commit is not None, (
        "upload_folder returned None — the manifest PUT did not produce a commit."
    )
    print(f"Uploaded {smoke_repo}@{session_revision}")


def test_06_manifest_lists_uploaded_files(logged_in, smoke_repo, session_revision, model_dir):
    """The revision's manifest names both files. Catches the case where the
    blobs land but the manifest is written with a missing or mangled
    `org.opencontainers.image.title` annotation — the upload looks fine and
    every subsequent download 404s."""
    _, expected = model_dir

    listed = set(hippius_hub.list_repo_files(smoke_repo, revision=session_revision))

    missing = set(expected) - listed
    assert not missing, (
        f"Manifest for {smoke_repo}@{session_revision} is missing {sorted(missing)} "
        f"— uploaded blobs are not addressable by filename. Listed: {sorted(listed)}"
    )


def test_07_download_model(logged_in, smoke_repo, session_revision, model_dir, download_cache):
    """Pull both files back into a cold cache and verify byte-for-byte.

    The hash comparison is the assertion that matters: it proves the whole
    chunked round-trip (CDC split, pack upload, ranged concurrent GET,
    reassembly at offset) preserved the bytes, not just that some file of the
    right length arrived.
    """
    _, expected = model_dir

    for filename, want in expected.items():
        local = hippius_hub.hf_hub_download(
            repo_id=smoke_repo,
            filename=filename,
            revision=session_revision,
            cache_dir=download_cache,
        )
        got = _sha256_of_file(local)
        assert got == want, (
            f"Downloaded {filename} from {smoke_repo}@{session_revision} does not "
            f"match what was uploaded: sha256 {got[:16]}... != {want[:16]}... — "
            f"the round-trip corrupted or truncated the file."
        )
        print(f"Round-tripped {filename} (sha256={want[:12]}...)")


def test_08_missing_file_raises(logged_in, smoke_repo, session_revision, download_cache):
    """A file that isn't in the manifest raises EntryNotFoundError rather than
    hanging or returning an empty file. Cheap, but it's the difference between
    a user seeing a clear error and a silently zero-byte model."""
    with pytest.raises(hippius_hub.EntryNotFoundError):
        hippius_hub.hf_hub_download(
            repo_id=smoke_repo,
            filename="definitely-not-in-this-revision.bin",
            revision=session_revision,
            cache_dir=download_cache,
        )
