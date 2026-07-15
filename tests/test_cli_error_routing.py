"""Regression: CLI maps typed errors to distinct exit codes (audit H3).

Before this fix, every download/upload exception collapsed to the same
opaque ``Download failed: <str(e)>`` with exit code 1. CI consumers and
shell scripts had no way to branch on the failure mode. The helper under
test (`_format_download_error`) routes each documented HF typed error to
a distinct exit code so wrappers can react (retry on concurrent-write,
prompt for auth on gated/disabled, etc.).
"""
import httpx
import pytest

from hippius_hub.cli import _format_download_error
from hippius_hub.errors import (
    ConcurrentManifestUpdateError,
    DisabledRepoError,
    EntryNotFoundError,
    GatedRepoError,
    HfHubHTTPError,
    LocalEntryNotFoundError,
    LocalTokenNotFoundError,
    RepositoryNotFoundError,
    RevisionNotFoundError,
)


def _httpx_status_error(status_code: int) -> httpx.HTTPStatusError:
    """A raw `httpx.HTTPStatusError` as `delete_repo` → Harbor raises it —
    NOT an HF-typed error. `raise_for_status()` produces exactly this."""
    request = httpx.Request("DELETE", "about:blank")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("boom", request=request, response=response)


def _synthetic_response(status_code: int = 404) -> httpx.Response:
    """Build the minimal httpx.Response that HfHubHTTPError.__init__ accepts.

    The HF typed errors (RepositoryNotFoundError, RevisionNotFoundError,
    GatedRepoError, DisabledRepoError, HfHubHTTPError) require a non-None
    ``response`` kwarg because HfHubHTTPError reads headers off it. We
    don't care about the body — only that the isinstance routing matches.
    """
    return httpx.Response(
        status_code=status_code,
        request=httpx.Request("GET", "about:blank"),
    )


@pytest.mark.parametrize(
    "exc, expected_code",
    [
        # EntryNotFoundError / LocalEntryNotFoundError are plain Exception
        # subclasses in huggingface_hub — they take a bare string.
        (EntryNotFoundError("missing.bin"), 10),
        (LocalEntryNotFoundError("missing-from-cache"), 13),
        # The rest subclass HfHubHTTPError and require a response kwarg.
        (RepositoryNotFoundError("no-such-repo", response=_synthetic_response(404)), 11),
        (RevisionNotFoundError("no-such-rev", response=_synthetic_response(404)), 12),
        (GatedRepoError("gated", response=_synthetic_response(403)), 14),
        (DisabledRepoError("disabled", response=_synthetic_response(403)), 14),
        (HfHubHTTPError("generic-http", response=_synthetic_response(500)), 16),
        # LocalTokenNotFoundError is an OSError (no saved credential): access-denied.
        (LocalTokenNotFoundError("no token"), 14),
        # Raw httpx errors from delete_repo → Harbor, mapped by status.
        (_httpx_status_error(401), 14),
        (_httpx_status_error(403), 14),
        (_httpx_status_error(404), 11),
        (_httpx_status_error(500), 16),
        # Fallback: anything not in the typed hierarchy still gets a code.
        (Exception("opaque"), 1),
    ],
)
def test_format_download_error_distinguishes_typed_errors(exc, expected_code):
    _, code = _format_download_error(exc)
    assert code == expected_code, f"{type(exc).__name__} should map to {expected_code}, got {code}"


def test_missing_local_token_routes_to_14_not_generic():
    """A missing credential must not collapse to generic exit 1 — the CLI needs
    to tell the user to log in. delete_repo raises LocalTokenNotFoundError for
    this (as create_repo does), and the message must point at `login`."""
    msg, code = _format_download_error(LocalTokenNotFoundError("no token"))
    assert code == 14
    assert "login" in msg.lower()


def test_raw_httpx_403_is_access_denied_not_generic():
    """delete_repo raises raw httpx.HTTPStatusError (not HfHubHTTPError). A 403
    means the token lacks push-delete/admin; it must route to 14, not 1, so a
    user with a read-only token gets an actionable message."""
    msg, code = _format_download_error(_httpx_status_error(403))
    assert code == 14
    assert "Access denied" in msg


def test_raw_httpx_404_is_repository_not_found_not_generic():
    msg, code = _format_download_error(_httpx_status_error(404))
    assert code == 11
    assert "Repository not found" in msg


def test_gated_repo_routes_to_14_not_11():
    """GatedRepoError IS-A RepositoryNotFoundError in huggingface_hub.

    A naive ordering that checks RepositoryNotFoundError first would route
    every gated repo to code 11 (not-found) instead of 14 (access denied),
    making auth failures indistinguishable from typos. This test pins the
    correct ordering: subclass checks must come before parent checks.
    """
    err = GatedRepoError("dataset-requires-license", response=_synthetic_response(403))
    msg, code = _format_download_error(err)
    assert code == 14
    assert "Access denied" in msg


def test_concurrent_update_routes_to_15_before_generic_http():
    """ConcurrentManifestUpdateError subclasses HfHubHTTPError; the typed
    routing must hit the specific code (15) not the generic one (16).

    The 412-precondition-failed message is actionable (retry or serialize
    externally); a generic 'Registry HTTP error' would hide that the fix
    is at the caller's concurrency model, not the network.
    """
    err = ConcurrentManifestUpdateError("manifest at o/r:main changed")
    msg, code = _format_download_error(err)
    assert code == 15
    assert "Concurrent write detected" in msg
    # Sanity: the error really is in the HfHubHTTPError hierarchy, so the
    # ordering matters — confirm it's not just a coincidence.
    assert isinstance(err, HfHubHTTPError)


def test_disabled_repo_is_not_subclass_of_repository_not_found():
    """Pin the asymmetry in HF's hierarchy: GatedRepoError subclasses
    RepositoryNotFoundError, but DisabledRepoError subclasses HfHubHTTPError
    directly. If HF ever harmonizes these, _format_download_error's ordering
    rationale (in the docstring) needs to be revisited.
    """
    from hippius_hub.errors import (
        DisabledRepoError, GatedRepoError, RepositoryNotFoundError,
    )
    assert issubclass(GatedRepoError, RepositoryNotFoundError)
    assert not issubclass(DisabledRepoError, RepositoryNotFoundError)
