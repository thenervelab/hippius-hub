"""dry_run on snapshot_download must be a true I/O short-circuit.

The fix moves the dry_run check before the token-service call and the
manifest GET. The pre-fix code would leak a 401/timeout/auth failure into
a path the caller asked to be a no-op. This file pins both:

  - snapshot_download(dry_run=True) returns the snapshot dir.
  - No HTTP calls of any kind go out (token endpoint, manifest GET,
    blob HEAD) under dry_run.

respx is used in strict mode (default: `assert_all_mocked=True`) with
zero routes registered. Any HTTP attempt fails the test with
`AllMockedAssertionError` — that's exactly the assertion we want.
"""
from __future__ import annotations

import os

import pytest
import respx

from hippius_hub import snapshot_download


REGISTRY = "https://registry.hippius.test"


@pytest.fixture(autouse=True)
def _point_registry_at_mock(monkeypatch):
    """Same dual-binding patch as test_upload_if_match.py — see comment there."""
    monkeypatch.setattr("hippius_hub.constants.DEFAULT_REGISTRY_URL", REGISTRY)
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_REGISTRY_URL", REGISTRY)


@respx.mock
def test_dry_run_makes_no_http_calls(tmp_path):
    """No mocked routes + strict respx = any HTTP call raises.

    If snapshot_download regresses and starts hitting the network under
    dry_run, respx's AllMockedAssertionError trips on the very first
    request — that's the behavioral pin we want.
    """
    cache_dir = tmp_path / "cache"
    result = snapshot_download(
        repo_id="owner/repo",
        revision="main",
        cache_dir=str(cache_dir),
        dry_run=True,
        token="literal-token",
    )

    # No routes were registered — if any call had fired, respx would have
    # raised before we got here.
    assert respx.mock.calls.call_count == 0, (
        f"dry_run=True must not hit the network; saw "
        f"{respx.mock.calls.call_count} HTTP call(s)"
    )

    expected_dir = os.path.join(
        str(cache_dir),
        # _cache_dirname builds the HF-shaped models--owner--repo name.
        "models--owner--repo",
        "snapshots",
        "main",
    )
    assert result == expected_dir


@respx.mock
def test_dry_run_with_anonymous_token_makes_no_http_calls(tmp_path):
    """token=False (HF anonymous sentinel) must not provoke a token-service
    call under dry_run any more than a literal token does."""
    cache_dir = tmp_path / "cache"
    result = snapshot_download(
        repo_id="owner/repo",
        revision="main",
        cache_dir=str(cache_dir),
        dry_run=True,
        token=False,
    )
    assert respx.mock.calls.call_count == 0
    assert "models--owner--repo" in result


@respx.mock
def test_dry_run_with_local_dir_returns_local_dir(tmp_path):
    """local_dir takes precedence over the cache-derived snapshot path,
    same as in the non-dry_run path."""
    local = tmp_path / "elsewhere"
    result = snapshot_download(
        repo_id="owner/repo",
        revision="main",
        local_dir=str(local),
        dry_run=True,
        token="literal-token",
    )
    assert respx.mock.calls.call_count == 0
    assert result == str(local)
