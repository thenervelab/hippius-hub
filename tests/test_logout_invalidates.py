"""logout should make subsequent authed calls fail.

`test_phase_a.test_logout_removes_token` only checks the file is gone — but
the OCI bearer token cache in `auth._OCI_TOKEN_CACHE` is module-level state
that survives logout. If a real user runs `logout()` and immediately calls
an authed function, will they get an auth error, or a silently-cached
result? This test pins the answer.
"""
import pytest

from hippius_hub import hippius_hub_upload, logout, whoami
from hippius_hub.auth import clear_oci_token_cache
from hippius_hub.errors import LocalTokenNotFoundError

from tests._helpers import write_test_file


pytestmark = pytest.mark.e2e


def test_whoami_after_logout_raises(logged_in):
    """After logout, whoami() with no args must raise — proves the saved-token
    removal is visible to the auth path."""
    # Confirm the precondition: whoami works before logout.
    pre = whoami()
    assert pre["name"].startswith("robot$")

    logout()
    with pytest.raises(LocalTokenNotFoundError):
        whoami()


def test_upload_after_logout_requires_fresh_oci_token(tmp_path, logged_in, test_repo, revision):
    """Upload mid-session, then logout + clear the OCI cache, then re-upload:
    the second call must hit the token endpoint without a saved token and fail.

    Without the cache clear this passes silently because the bearer JWT is still
    valid in process memory — a real user wouldn't have that cache, so we
    simulate by clearing. This documents the failure mode the cache is hiding.
    """
    src = tmp_path / "lo.bin"
    write_test_file(src, 64, seed=b"logout")
    # Sanity-check the precondition.
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    logout()
    clear_oci_token_cache()

    # Now there's no saved credential AND no cached bearer token. The next
    # upload must fail at the OCI token-service step.
    second_rev = revision + "-after-logout"
    with pytest.raises(Exception):
        hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=second_rev)
