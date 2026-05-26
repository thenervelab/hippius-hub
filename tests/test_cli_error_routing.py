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
    RepositoryNotFoundError,
    RevisionNotFoundError,
)


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
        (EntryNotFoundError("missing.bin"), 2),
        (LocalEntryNotFoundError("missing-from-cache"), 5),
        # The rest subclass HfHubHTTPError and require a response kwarg.
        (RepositoryNotFoundError("no-such-repo", response=_synthetic_response(404)), 3),
        (RevisionNotFoundError("no-such-rev", response=_synthetic_response(404)), 4),
        (GatedRepoError("gated", response=_synthetic_response(403)), 6),
        (DisabledRepoError("disabled", response=_synthetic_response(403)), 6),
        (HfHubHTTPError("generic-http", response=_synthetic_response(500)), 8),
        # Fallback: anything not in the typed hierarchy still gets a code.
        (Exception("opaque"), 1),
    ],
)
def test_format_download_error_distinguishes_typed_errors(exc, expected_code):
    _, code = _format_download_error(exc)
    assert code == expected_code, f"{type(exc).__name__} should map to {expected_code}, got {code}"


def test_gated_repo_routes_to_6_not_3():
    """GatedRepoError IS-A RepositoryNotFoundError in huggingface_hub.

    A naive ordering that checks RepositoryNotFoundError first would route
    every gated repo to code 3 (not-found) instead of 6 (access denied),
    making auth failures indistinguishable from typos. This test pins the
    correct ordering: subclass checks must come before parent checks.
    """
    err = GatedRepoError("dataset-requires-license", response=_synthetic_response(403))
    msg, code = _format_download_error(err)
    assert code == 6
    assert "Access denied" in msg


def test_concurrent_update_routes_to_7_before_generic_http():
    """ConcurrentManifestUpdateError subclasses HfHubHTTPError; the typed
    routing must hit the specific code (7) not the generic one (8).

    The 412-precondition-failed message is actionable (retry or serialize
    externally); a generic 'Registry HTTP error' would hide that the fix
    is at the caller's concurrency model, not the network.
    """
    err = ConcurrentManifestUpdateError("manifest at o/r:main changed")
    msg, code = _format_download_error(err)
    assert code == 7
    assert "Concurrent write detected" in msg
    # Sanity: the error really is in the HfHubHTTPError hierarchy, so the
    # ordering matters — confirm it's not just a coincidence.
    assert isinstance(err, HfHubHTTPError)
