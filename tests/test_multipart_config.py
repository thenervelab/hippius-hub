"""Feature-gate config for the parallel multipart upload path.

These resolvers are the ON/OFF switch for the receiver route: when
`HIPPIUS_RECEIVER_URL` is unset the path is entirely disabled and uploads must
behave exactly as they do today. The tests pin that gate plus the edges of the
env parsing (non-positive threshold, empty-string receiver URL) that would
otherwise produce a degenerate transfer or a malformed request URL.
"""
import pytest

from hippius_hub.constants import (
    DEFAULT_MULTIPART_THRESHOLD,
    resolve_multipart_threshold,
    resolve_receiver_url,
)


def test_threshold_defaults_to_256mb(monkeypatch):
    monkeypatch.delenv("HIPPIUS_MULTIPART_THRESHOLD", raising=False)
    assert resolve_multipart_threshold() == DEFAULT_MULTIPART_THRESHOLD
    assert DEFAULT_MULTIPART_THRESHOLD == 256 * 1024 * 1024


def test_threshold_env_override(monkeypatch):
    monkeypatch.setenv("HIPPIUS_MULTIPART_THRESHOLD", str(64 * 1024 * 1024))
    assert resolve_multipart_threshold() == 64 * 1024 * 1024


def test_threshold_rejects_non_positive(monkeypatch):
    # A zero/negative threshold would route every (or no) blob incoherently;
    # _resolve_positive_int surfaces it immediately rather than degrading.
    monkeypatch.setenv("HIPPIUS_MULTIPART_THRESHOLD", "0")
    with pytest.raises(ValueError):
        resolve_multipart_threshold()


def test_receiver_url_none_when_unset(monkeypatch):
    monkeypatch.delenv("HIPPIUS_RECEIVER_URL", raising=False)
    assert resolve_receiver_url() is None


def test_receiver_url_empty_is_disabled(monkeypatch):
    # `HIPPIUS_RECEIVER_URL=` in a profile must disable, not yield a "" base
    # that produces requests to "/v2/...".
    monkeypatch.setenv("HIPPIUS_RECEIVER_URL", "   ")
    assert resolve_receiver_url() is None


def test_receiver_url_trims_trailing_slash(monkeypatch):
    monkeypatch.setenv("HIPPIUS_RECEIVER_URL", "https://receiver.hippius.svc:8080/")
    assert resolve_receiver_url() == "https://receiver.hippius.svc:8080"


def test_receiver_url_rejects_http_to_remote_host(monkeypatch):
    # The client forwards a repo-scoped Harbor push token to the receiver; an
    # http:// hop to a remote host would leak it in cleartext, so reject loudly.
    monkeypatch.setenv("HIPPIUS_RECEIVER_URL", "http://receiver.hippius.svc:8080")
    with pytest.raises(ValueError, match="cleartext"):
        resolve_receiver_url()


def test_receiver_url_allows_http_localhost(monkeypatch):
    # Local port-forward testing over http://localhost must stay usable.
    monkeypatch.setenv("HIPPIUS_RECEIVER_URL", "http://localhost:8080/")
    assert resolve_receiver_url() == "http://localhost:8080"


def test_receiver_url_rejects_non_http_scheme(monkeypatch):
    monkeypatch.setenv("HIPPIUS_RECEIVER_URL", "ftp://receiver.hippius.svc")
    with pytest.raises(ValueError, match="http"):
        resolve_receiver_url()
