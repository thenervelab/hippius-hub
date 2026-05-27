"""Live test for audit C2: token=False = HF anonymous sentinel, end-to-end.

The localhost test `test_anonymous_download.py` pins the C2 invariants
(no Authorization header on token-service request, no docker-config
fallback) against respx. This file is the live counterpart: pulls a
public file from `test/e2e-client` with `token=False` and asserts the
download succeeds.

If the test repo isn't actually public (or the registry rejects the
anonymous pull for any reason), this test skips with a clear message
rather than hard-failing — anonymous access is a registry-config
property, not a hippius_hub property, and a misconfigured registry
shouldn't block other e2e tests.
"""
from __future__ import annotations

import os

import pytest

from hippius_hub import hippius_hub_upload, hf_hub_download
from hippius_hub.errors import GatedRepoError, HfHubHTTPError, RepositoryNotFoundError

from tests._helpers import sha256_of_file, write_test_file


pytestmark = pytest.mark.e2e


def test_token_false_can_pull_public_file(
    tmp_path, cache_dir, logged_in, test_repo, revision,
):
    """Upload a file (authenticated), then download it anonymously.

    Uses `logged_in` for the upload — anonymous WRITE is never
    supported. The download then uses `token=False` to verify the
    anonymous pull path works against `test/e2e-client`. If the repo
    is private, the registry returns 401/403 and the test skips with
    a precise diagnostic rather than a generic failure.
    """
    src = tmp_path / "anon.bin"
    expected_sha = write_test_file(src, 2048, seed=b"anon-pull")

    hippius_hub_upload(
        repo_id=test_repo,
        local_path=str(src),
        revision=revision,
    )

    # Use a sibling cache so we don't accidentally cache-hit on a
    # previous authenticated download.
    anon_cache = os.path.join(cache_dir, "anon")
    os.makedirs(anon_cache, exist_ok=True)

    try:
        out = hf_hub_download(
            repo_id=test_repo,
            filename="anon.bin",
            revision=revision,
            cache_dir=anon_cache,
            token=False,  # HF anonymous sentinel
        )
    except (RepositoryNotFoundError, GatedRepoError) as e:
        pytest.skip(
            f"`{test_repo}` does not appear to be public on the live "
            f"registry (got {type(e).__name__}: {e}); the C2 invariant "
            f"is still pinned at the localhost level via "
            f"tests/test_anonymous_download.py. Mark the repo public "
            f"via `hippius-hub registry publicity public` to enable "
            f"this live test."
        )
    except HfHubHTTPError as e:
        # 401 from the token service when anonymous isn't accepted — same
        # outcome (test repo isn't open to anonymous pulls).
        if "401" in str(e) or "403" in str(e):
            pytest.skip(
                f"registry rejected anonymous pull from `{test_repo}` "
                f"({e}); see above for fix."
            )
        raise

    assert sha256_of_file(out) == expected_sha
