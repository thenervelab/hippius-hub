"""End-to-end coverage of the per-project robot key lifecycle.

The `registry keys *` subtree (added in 4430fa9) has zero tests. Each role
preset (`read`, `push`, `push-delete`, `admin`) maps onto a different Harbor
ACL set — a regression in the role wiring is invisible until a user with
that key tries (and fails or wrongly succeeds) to do something.

Strategy: parametrize over roles, create → show → rotate → revoke for each.
Cleanup fixture revokes any leftover `e2e-*` key so a failing run can't leak
robots into the test account.
"""
import uuid

import pytest

from hippius_hub import console
from hippius_hub.console import ConsoleError


pytestmark = pytest.mark.e2e


@pytest.fixture
def cleanup_e2e_keys(console_logged_in):
    """Best-effort teardown: revoke any leftover `e2e-*` key after the test."""
    yield
    try:
        for k in console.list_keys() or []:
            if k.get("name", "").startswith("e2e-"):
                try:
                    console.revoke_key(k["id"])
                except Exception:
                    pass
    except Exception:
        pass


@pytest.mark.parametrize("role", ["read", "push", "push-delete", "admin"])
def test_key_lifecycle_per_role(console_logged_in, cleanup_e2e_keys, role):
    """create → show → rotate → revoke for each role preset.

    The secret is returned by create / rotate exactly once — losing it means
    the user must rotate again. Validates both that the secret is present
    and that show() correctly omits it.
    """
    name = f"e2e-{uuid.uuid4().hex[:8]}"

    # create
    created = console.create_key(name, role)
    assert created["name"] == name
    assert created["role"] == role
    assert created.get("secret"), "create_key must return the secret exactly once"
    assert created.get("login"), "create_key must return the robot login"
    key_id = created["id"]

    # show — secret must NOT come back, only metadata.
    shown = console.show_key(key_id)
    assert shown["id"] == key_id
    assert shown["name"] == name
    assert shown["role"] == role
    assert not shown.get("secret"), "show_key must not echo the secret"

    # rotate — fresh secret, same id and role.
    rotated = console.rotate_key(key_id)
    assert rotated["id"] == key_id
    assert rotated["role"] == role
    assert rotated.get("secret"), "rotate_key must return the new secret"
    assert rotated.get("secret") != created.get("secret"), "rotate_key must change the secret"

    # revoke + verify gone.
    console.revoke_key(key_id)
    remaining_ids = {k["id"] for k in (console.list_keys() or [])}
    assert key_id not in remaining_ids, "revoked key must not appear in list_keys"


def test_create_key_with_expires_days(console_logged_in, cleanup_e2e_keys):
    """`--expires-days` is the CLI's only optional kwarg; verify the server
    accepts it and stores an `expires_at` timestamp."""
    name = f"e2e-{uuid.uuid4().hex[:8]}"
    created = console.create_key(name, "read", expires_days=30)
    try:
        assert created.get("expires_at"), "expires_days should populate expires_at"
    finally:
        console.revoke_key(created["id"])


def test_revoke_unknown_key_is_404(console_logged_in):
    """Revoking a never-existed id surfaces a typed 404, not a 5xx."""
    with pytest.raises(ConsoleError) as excinfo:
        # 2**31-1 is well past any realistic key id.
        console.revoke_key(2_147_483_646)
    assert excinfo.value.status_code in (404, 400)
