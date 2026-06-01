"""Rotate the project's bootstrap robot secret. Marked `expensive` because:

  - The old secret stops working immediately, so any parallel test runner
    holding the previous docker creds will start failing.
  - Recovering requires `registry rotate-token --docker-login` again.

Opt-in via `HIPPIUS_TEST_ALLOW_TOKEN_ROTATE=1`. Always re-persists the
fresh secret to the test's tmp HOME so subsequent same-process tests work.
"""
import base64
import os

import pytest

from hippius_hub import console
from hippius_hub.auth import clear_oci_token_cache


pytestmark = [pytest.mark.e2e, pytest.mark.expensive]


@pytest.fixture(autouse=True)
def _require_token_rotate_opt_in():
    if os.environ.get("HIPPIUS_TEST_ALLOW_TOKEN_ROTATE") != "1":
        pytest.skip("Set HIPPIUS_TEST_ALLOW_TOKEN_ROTATE=1 to enable robot rotation")


def test_rotate_robot_issues_fresh_secret(console_logged_in, logged_in, tmp_path, monkeypatch):
    """rotate_robot returns a new (login, secret) pair. Re-derive the Basic
    header from the new secret and persist it so the rest of the session
    can still push/pull.

    Side effect that's hard to avoid: the previous secret in `~/.docker/config.json`
    is now stale. Outside CI, the user must rerun `registry rotate-token --docker-login`."""
    me_before = console.me()
    old_login = me_before.get("robot_login")
    assert old_login, "test account must already have a provisioned project"

    rotated = console.rotate_robot()
    assert rotated.get("robot_login") == old_login, (
        "rotation must keep the same login, only change the secret"
    )
    new_secret = rotated.get("robot_secret")
    assert new_secret, "rotate_robot must return the new secret exactly once"

    # Re-persist the fresh secret so any later test in this run still works.
    from hippius_hub import auth
    new_basic = base64.b64encode(f"{old_login}:{new_secret}".encode()).decode()
    with open(auth.TOKEN_PATH, "w") as f:
        f.write(f"Basic {new_basic}")
    clear_oci_token_cache()
