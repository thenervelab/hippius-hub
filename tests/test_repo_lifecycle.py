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

from tests._helpers import run_cli, write_test_file


pytestmark = pytest.mark.e2e


def _skip_if_no_admin(exc: httpx.HTTPStatusError):
    """`delete_repo` hits Harbor's /api/v2.0/ admin API, which 403s for robot
    accounts that lack project_admin (the CI robot is push-only by default).
    Map 403 → skip with a clear reason so the test is still useful when the
    suite runs under an admin credential locally."""
    if exc.response.status_code == 403:
        pytest.skip(
            "delete_repo requires Harbor project_admin perms; "
            "current credentials are push-only. Run under an admin token to exercise."
        )


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

    try:
        delete_repo(disposable_repo)
    except httpx.HTTPStatusError as e:
        _skip_if_no_admin(e)
        raise
    assert repo_exists(disposable_repo) is False


def test_cli_repos_delete_roundtrip(tmp_path, cli_env, logged_in, test_repo):
    """The `registry repos delete <repo> --yes` CLI command must remove a repo
    end-to-end. Set up (create + upload) uses the Python API; the delete goes
    through the real CLI subprocess so this covers arg plumbing, the --yes
    prompt bypass, and the success path — not just `delete_repo` itself.

    Skips (after cleaning up via the Python API) when the credentials lack
    Harbor project_admin, same constraint as the Python-level test."""
    repo_id = f"{test_repo.split('/')[0]}/cli-del-{uuid.uuid4().hex[:8]}"
    src = tmp_path / "d.bin"
    write_test_file(src, 128, seed=b"cli-delete")

    create_repo(repo_id, exist_ok=True)
    upload_file(path_or_fileobj=str(src), path_in_repo="d.bin",
                repo_id=repo_id, revision="main")
    assert repo_exists(repo_id) is True

    r = run_cli(["registry", "repos", "delete", repo_id, "--yes"],
                env=cli_env, check=False)
    if r.returncode == 1 and "admin permissions" in r.stdout:
        delete_repo(repo_id, missing_ok=True)  # best-effort cleanup
        pytest.skip("credentials lack Harbor project_admin; can't exercise CLI delete")
    assert r.returncode == 0, f"stdout={r.stdout!r} stderr={r.stderr!r}"
    assert "Repo deleted" in r.stdout
    assert repo_exists(repo_id) is False


def test_cli_repos_delete_missing_ok_is_silent(cli_env, test_repo):
    """`--missing-ok` on a never-created repo must exit 0 (HF parity), whereas
    without it the CLI maps Harbor's 404 to exit 11. 403 (no admin) short-
    circuits both paths, so accept it as a skip signal."""
    ghost = f"{test_repo.split('/')[0]}/ghost-{uuid.uuid4().hex[:8]}"
    r = run_cli(["registry", "repos", "delete", ghost, "--yes", "--missing-ok"],
                env=cli_env, check=False)
    if r.returncode == 1 and "admin permissions" in r.stdout:
        pytest.skip("credentials lack Harbor project_admin; can't exercise CLI delete")
    assert r.returncode == 0, f"stdout={r.stdout!r} stderr={r.stderr!r}"


def test_delete_missing_repo_missing_ok_true_is_silent(logged_in, test_repo):
    """When `missing_ok=True`, deleting a never-pushed repo must not raise —
    this is the HF semantic and the wrapper around `harbor_delete_repository`."""
    ghost = f"{test_repo.split('/')[0]}/ghost-{uuid.uuid4().hex[:8]}"
    try:
        delete_repo(ghost, missing_ok=True)
    except httpx.HTTPStatusError as e:
        _skip_if_no_admin(e)
        raise


def test_delete_missing_repo_missing_ok_false_raises(logged_in, test_repo):
    """Default (`missing_ok=False`) surfaces the raw httpx error — `delete_repo`
    intentionally does not translate to RepositoryNotFoundError today (it's a
    known HF-parity divergence). 404 (missing) and 403 (no admin) are both
    valid raise paths; this test pins that *some* HTTPStatusError surfaces
    rather than a silent success."""
    ghost = f"{test_repo.split('/')[0]}/ghost-{uuid.uuid4().hex[:8]}"
    with pytest.raises(httpx.HTTPStatusError):
        delete_repo(ghost, missing_ok=False)
