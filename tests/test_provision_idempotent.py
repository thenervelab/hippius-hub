"""Verify `console.provision` is idempotent for an already-provisioned account.

The destructive paths (fresh creation, credit debit) need a sandbox we
don't have. The idempotent path doesn't — the server detects the existing
project and returns `{"idempotent": True, ...}` without creating new state
or charging credits. That branch is what the CLI's "already exists" hint
relies on, and it has no test today.
"""
import pytest

from hippius_hub import console


pytestmark = pytest.mark.e2e


def test_provision_existing_namespace_is_idempotent(
    console_logged_in, console_test_project,
):
    """Re-provisioning the active project must return `idempotent=True` with
    project_name + plan_name, NOT a fresh robot_secret (the CLI tells the
    user to rotate to retrieve one if they need it)."""
    res = console.provision(console_test_project)
    assert res.get("idempotent") is True, f"expected idempotent=True, got {res!r}"
    assert res.get("project_name") == console_test_project
    # Plan name is read by the CLI's idempotent-branch print. Server-side
    # default is to include it, so assert it's present (even if empty).
    assert "plan_name" in res
    # The robot secret is encrypted at rest; the server intentionally does
    # NOT echo it on idempotent calls. Verify so we catch a regression that
    # accidentally exposes it.
    assert not res.get("robot_secret"), (
        "idempotent provision must not leak the robot secret"
    )
