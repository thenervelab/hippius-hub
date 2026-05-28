"""End-to-end coverage of the repo lifecycle: create_repo → upload → delete_repo.

`delete_repo` is the only repo-mutating module function with no live test —
test_phase_b.py only covers the unit-level `rejects unknown repo_type` path.
This file closes that gap with a single round trip per repo type that we
control. Uses uuid-tagged names so concurrent CI runs don't collide and so
re-runs of a single repo never leak shared state.
"""
import uuid

import httpx
import pytest

from hippius_hub import (
    create_repo,
    delete_repo,
    repo_exists,
    upload_file,
)

from tests._helpers import write_test_file


pytestmark = pytest.mark.e2e


@pytest.fixture
def disposable_repo(logged_in, test_repo):
    """A repo_id under the existing `test/` namespace, suffixed with a uuid so
    each test owns a fresh OCI repository. Yields the id; teardown deletes the
    repo even if the test raised mid-flight."""
    repo_id = f"{test_repo.split('/')[0]}/lifecycle-{uuid.uuid4().hex[:8]}"
    yield repo_id
    try:
        delete_repo(repo_id, missing_ok=True)
    except Exception:
        # Best-effort cleanup; surfacing here would mask the real test failure.
        pass


def test_create_upload_delete_roundtrip(tmp_path, disposable_repo):
    """The full lifecycle. After delete_repo, repo_exists must flip to False —
    proves both that the create code path produces a usable repo and that
    delete actually unwinds it from the registry's perspective."""
    src = tmp_path / "lc.bin"
    write_test_file(src, 128, seed=b"lifecycle")

    url = create_repo(disposable_repo, exist_ok=True)
    assert str(url).endswith(disposable_repo)

    upload_file(
        path_or_fileobj=str(src),
        path_in_repo="lc.bin",
        repo_id=disposable_repo,
        revision="main",
    )
    assert repo_exists(disposable_repo) is True

    delete_repo(disposable_repo)
    assert repo_exists(disposable_repo) is False


def test_delete_missing_repo_missing_ok_true_is_silent(logged_in, test_repo):
    """When `missing_ok=True`, deleting a never-pushed repo must not raise —
    this is the HF semantic and the wrapper around `harbor_delete_repository`."""
    ghost = f"{test_repo.split('/')[0]}/ghost-{uuid.uuid4().hex[:8]}"
    delete_repo(ghost, missing_ok=True)


def test_delete_missing_repo_missing_ok_false_raises(logged_in, test_repo):
    """Default (`missing_ok=False`) surfaces the raw httpx 404 — `delete_repo`
    intentionally does not translate to RepositoryNotFoundError today (it's a
    known HF-parity divergence). This test pins the current shape; if/when the
    wrapper starts typing the error, update the assertion."""
    ghost = f"{test_repo.split('/')[0]}/ghost-{uuid.uuid4().hex[:8]}"
    with pytest.raises(httpx.HTTPStatusError):
        delete_repo(ghost, missing_ok=False)
