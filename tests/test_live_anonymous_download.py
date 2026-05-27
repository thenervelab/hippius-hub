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

import httpx
import pytest

from hippius_hub import hippius_hub_upload, hf_hub_download
from hippius_hub.errors import GatedRepoError, HfHubHTTPError, RepositoryNotFoundError

from tests._helpers import sha256_of_file, write_test_file


# Status codes the live registry returns when the test repo is private
# and the request was anonymous. The skip path below treats these as
# "not-a-bug-in-hippius_hub" — the repo is private, anonymous can't
# read it. Mark the repo public via
# `hippius-hub registry publicity public` to flip the test from skip
# to assert.
_PRIVATE_REPO_STATUS_CODES = {401, 403, 404}


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
        # HF-typed exceptions get the clearest skip reason.
        pytest.skip(_skip_reason(test_repo, e))
    except (HfHubHTTPError, httpx.HTTPStatusError) as e:
        # Raw httpx.HTTPStatusError can leak through _oci.fetch_manifest
        # (line 75: `resp.raise_for_status()`) when the registry returns
        # 401 to the manifest GET — no HF-type wrapper applies because
        # the failure happens before the file-not-found path. Inspect
        # the response status code instead of string-matching.
        status = getattr(getattr(e, "response", None), "status_code", None)
        if status in _PRIVATE_REPO_STATUS_CODES:
            pytest.skip(_skip_reason(test_repo, e, status=status))
        raise

    assert sha256_of_file(out) == expected_sha


def _skip_reason(test_repo: str, exc: BaseException, *, status: int | None = None) -> str:
    code = f" [HTTP {status}]" if status is not None else ""
    return (
        f"`{test_repo}` does not appear to be public on the live registry "
        f"(got {type(exc).__name__}{code}: {exc}). The C2 invariant is "
        f"still pinned at the localhost level via "
        f"tests/test_anonymous_download.py. Mark the repo public via "
        f"`hippius-hub registry publicity public` to enable this live test."
    )
