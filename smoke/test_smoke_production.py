"""Hourly production smoke suite for the published hippius_hub client.

Answers one question every hour: can a user who just ran `pip install hippius_hub`
still read models from registry.hippius.com?

Since the registry's projects moved from Harbor to the hub (production cutover plan,
hippius-hub-rust docs/plans/2026-11-18-production-cutover-plan.md, W11), this client's
reads are the part it keeps: the hub serves them through the same OCI paths. It refuses
the client's model pushes to a moved project, with a message pointing at huggingface_hub
(plan D2), so this suite no longer pushes. The hub's own write path is tested by its
end-to-end suite (hippius-hub-rust crates/hub-e2e), not here.

Every test is read-only and needs no cleanup: they share nothing and run in any order.
"""
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import hippius_hub

# A public artifact of ours on the hub (project `hipc-vm-test`, moved in cohort 0), pinned
# by its manifest digest so a moved tag cannot change what is checked. Read with the
# published client on 27 November 2026: one 37-byte file.
PUBLIC_REPO = "hipc-vm-test/hello-artifact"
PUBLIC_REVISION = "sha256:22461a02a2956ed5a51b281055364445692c1ef2b4c6848de36482ed9f718167"
PUBLIC_FILE = "artifact.txt"
PUBLIC_FILE_SHA256 = "26ebdb5222ed0f0dbc06821493ba9824c673d785711e00ead5549b80921347ec"


def _sha256_of_file(path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def test_01_published_client_loads(client_path):
    """The wheel under test is the one from PyPI, and its Rust extension
    imports. A broken abi3 wheel (wrong tag, missing .so) fails right here
    instead of surfacing as a confusing download error later."""
    from hippius_hub import hippius_core

    assert hippius_core is not None
    print(f"Testing hippius_hub {hippius_hub.__version__} from {client_path}")


def test_02_cli_entrypoint(client_path):
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


def test_03_anonymous_download(client_path, download_cache):
    """An anonymous user lists and downloads a public file, byte for byte.

    `token=False` is the client's anonymous mode: it does not fall back to a
    stored login, so this is what a user with no account gets.
    """
    files = hippius_hub.list_repo_files(PUBLIC_REPO, revision=PUBLIC_REVISION, token=False)
    assert files == [PUBLIC_FILE], (
        f"{PUBLIC_REPO}@{PUBLIC_REVISION} lists {files}, expected [{PUBLIC_FILE!r}]."
    )

    local = hippius_hub.hf_hub_download(
        repo_id=PUBLIC_REPO,
        filename=PUBLIC_FILE,
        revision=PUBLIC_REVISION,
        token=False,
        cache_dir=download_cache,
    )
    got = _sha256_of_file(local)
    assert got == PUBLIC_FILE_SHA256, (
        f"Downloaded {PUBLIC_FILE} from {PUBLIC_REPO}@{PUBLIC_REVISION} has sha256 "
        f"{got[:16]}..., expected {PUBLIC_FILE_SHA256[:16]}... — the registry served "
        f"different bytes for a pinned digest."
    )


def test_04_missing_file_raises(client_path, download_cache):
    """A file that isn't in the manifest raises EntryNotFoundError rather than
    hanging or returning an empty file."""
    with pytest.raises(hippius_hub.EntryNotFoundError):
        hippius_hub.hf_hub_download(
            repo_id=PUBLIC_REPO,
            filename="definitely-not-in-this-revision.bin",
            revision=PUBLIC_REVISION,
            token=False,
            cache_dir=download_cache,
        )


def test_05_private_read_needs_the_key(client_path, smoke_key, private_repo):
    """A private repo is hidden from anonymous users and readable with a `hip_` key.

    The anonymous half is what makes the keyed half mean something: if the repo
    were public, the key would never be checked. The repo must stay private and keep at
    least one revision: it holds the `smoke-*` revisions the old push suite left, imported
    from Harbor on 20 November 2026, and nothing pushes to it any more.
    """
    with pytest.raises(hippius_hub.RepositoryNotFoundError):
        hippius_hub.list_repo_refs(private_repo, token=False)

    refs = hippius_hub.list_repo_refs(private_repo, token=smoke_key)
    revisions = [r.name for r in refs.branches] + [t.name for t in refs.tags]
    assert revisions, (
        f"{private_repo} lists no revisions with HIPPIUS_SMOKE_KEY — the key was "
        f"accepted but the repo is empty, or the key cannot read it."
    )

    files = hippius_hub.list_repo_files(private_repo, revision=revisions[0], token=smoke_key)
    assert files, f"{private_repo}@{revisions[0]} lists no files with HIPPIUS_SMOKE_KEY."
