"""Audit L3: transient control-plane failures are retried like the data plane.

`_http.request_with_retry` mirrors the Rust `CoreError::is_retryable` (408/429 +
5xx + transport errors) so a Harbor blip during a rolling redeploy doesn't abort
an operation whose blob requests would have retried. `base_delay=0.0` keeps these
unit tests instant.
"""
from __future__ import annotations

import httpx
import respx

from hippius_hub import _http
from hippius_hub._oci import fetch_manifest
from tests.respx_fixtures import MOCK_REGISTRY


def test_retries_5xx_then_succeeds():
    n = {"c": 0}

    def send():
        n["c"] += 1
        return httpx.Response(503) if n["c"] < 3 else httpx.Response(200, text="ok")

    resp = _http.request_with_retry(send, base_delay=0.0)
    assert resp.status_code == 200
    assert n["c"] == 3


def test_gives_up_and_surfaces_last_response():
    n = {"c": 0}

    def send():
        n["c"] += 1
        return httpx.Response(503)

    resp = _http.request_with_retry(send, attempts=3, base_delay=0.0)
    assert resp.status_code == 503  # last attempt's response, for the caller to raise on
    assert n["c"] == 3


def test_does_not_retry_404():
    n = {"c": 0}

    def send():
        n["c"] += 1
        return httpx.Response(404)

    resp = _http.request_with_retry(send, base_delay=0.0)
    assert resp.status_code == 404
    assert n["c"] == 1  # a 404 is not transient — returned on the first attempt


def test_retries_transport_error_then_succeeds():
    n = {"c": 0}

    def send():
        n["c"] += 1
        if n["c"] < 2:
            raise httpx.ConnectError("boom")
        return httpx.Response(200)

    resp = _http.request_with_retry(send, base_delay=0.0)
    assert resp.status_code == 200
    assert n["c"] == 2


def test_transport_error_exhausts_and_raises():
    def send():
        raise httpx.ConnectError("down")

    try:
        _http.request_with_retry(send, attempts=2, base_delay=0.0)
    except httpx.ConnectError:
        return
    raise AssertionError("a persistent transport error must propagate after retries")


@respx.mock
def test_fetch_manifest_wires_through_retry_on_transient_5xx():
    # L3 WIRING (not just the helper in isolation): fetch_manifest runs before every
    # upload/download, so it must route its GET through request_with_retry. A 503
    # then 200 must yield the manifest after exactly one retry — inlining the GET
    # back to a bare `client().get()` (undoing L3) makes route.call_count == 1 and
    # fails this test.
    repo = "acme/model"
    route = respx.get(f"{MOCK_REGISTRY}/v2/{repo}/manifests/main").mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(
                200,
                json={"schemaVersion": 2, "layers": []},
                headers={"Docker-Content-Digest": "sha256:" + "d" * 64},
            ),
        ]
    )
    result = fetch_manifest(MOCK_REGISTRY, repo, "main", "tok")
    assert result is not None and result.manifest["schemaVersion"] == 2
    assert route.call_count == 2, "fetch_manifest must retry the 503 through request_with_retry"
