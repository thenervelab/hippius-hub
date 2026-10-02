"""Token handling and status reporting for the Harbor reachability sweep.

The sweep decides whether a migration rolls back, so a stale token must never
be reported as missing data. The 2026-08-27 baseline run against prod produced
~700 false failures for exactly that reason: repositories with thousands of
artifacts outlive Harbor's 30-minute token, and every probe after expiry 401'd.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SWEEP = (
    Path(__file__).resolve().parents[1]
    / "deploy"
    / "harbor-s3-prod"
    / "harbor_reachability_sweep.py"
)


def _load():
    spec = importlib.util.spec_from_file_location("harbor_reachability_sweep", _SWEEP)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def sweep():
    return _load()


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def close(self) -> None:
        pass


class FakeClient:
    """Mints a new token per /service/token call; GETs honour `statuses`."""

    def __init__(self, statuses: list[int]) -> None:
        self.statuses = list(statuses)
        self.minted = 0
        self.seen_tokens: list[str] = []

    def get(self, url, headers=None, params=None, follow_redirects=False):
        if url.endswith("/service/token"):
            self.minted += 1
            return FakeResponse(200, {"token": f"tok{self.minted}"})
        self.seen_tokens.append((headers or {}).get("Authorization", ""))
        return FakeResponse(self.statuses.pop(0))

    def head(self, *a, **kw):  # pragma: no cover - sweep no longer HEADs
        raise AssertionError("the sweep must GET, not HEAD")


# --- token refresh --------------------------------------------------------


def test_token_is_cached_across_probes(sweep):
    client = FakeClient([200, 200])
    tokens = sweep.TokenCache(client, "http://harbor", "basic")

    assert tokens.get("a/b") == "tok1"
    assert tokens.get("a/b") == "tok1"
    assert client.minted == 1


def test_refresh_mints_a_new_token(sweep):
    client = FakeClient([])
    tokens = sweep.TokenCache(client, "http://harbor", "basic")

    first = tokens.get("a/b")
    second = tokens.get("a/b", refresh=True)

    assert first == "tok1"
    assert second == "tok2"
    assert client.minted == 2


def test_a_401_is_retried_with_a_fresh_token(sweep):
    """The expiry case: first GET 401s, the retry succeeds on a new token."""
    client = FakeClient([401, 200])
    tokens = sweep.TokenCache(client, "http://harbor", "basic")

    status = sweep.fetch_with_refresh(
        client, tokens, "a/b", "http://harbor/v2/a/b/manifests/x"
    )

    assert status == 200
    assert client.minted == 2
    assert client.seen_tokens == ["Bearer tok1", "Bearer tok2"]


def test_a_404_is_not_retried(sweep):
    """A missing link is a real finding — do not burn a token mint on it."""
    client = FakeClient([404])
    tokens = sweep.TokenCache(client, "http://harbor", "basic")

    status = sweep.fetch_with_refresh(
        client, tokens, "a/b", "http://harbor/v2/a/b/manifests/x"
    )

    assert status == 404
    assert client.minted == 1


def test_probe_passes_when_the_refresh_rescues_it(sweep):
    """An expired token must not be reported as a broken artifact."""
    client = FakeClient([401, 200, 206])
    tokens = sweep.TokenCache(client, "http://harbor", "basic")

    result = sweep.probe(
        client,
        "http://harbor",
        tokens,
        ("a/b", "sha256:" + "ab" * 32, "sha256:" + "cd" * 32),
    )

    assert result is None


# --- status reporting -----------------------------------------------------


def test_404_reads_as_missing_data(sweep):
    assert "missing" in sweep._why(404, "_layers")


@pytest.mark.parametrize("status", [401, 403])
def test_auth_failures_never_claim_data_is_missing(sweep, status):
    """The exact bug the prod baseline hit: 401 reported as a missing link."""
    reason = sweep._why(status, "_manifests/revisions")

    assert "missing" not in reason
    assert "auth" in reason.lower()


def test_unexpected_status_does_not_pretend_to_know(sweep):
    reason = sweep._why(500, "_layers")

    assert "missing" not in reason
    assert "investigate" in reason


def test_a_persistent_401_is_still_reported_as_a_failure(sweep):
    """Refresh is one retry, not an infinite excuse — a real auth break surfaces."""
    client = FakeClient([401, 401])
    tokens = sweep.TokenCache(client, "http://harbor", "basic")

    result = sweep.probe(
        client,
        "http://harbor",
        tokens,
        ("a/b", "sha256:" + "ab" * 32, None),
    )

    assert result is not None
    subject, reason = result
    assert "401" in reason
    assert "missing" not in reason
