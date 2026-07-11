"""Audit L3: transient control-plane failures are retried like the data plane.

`_http.request_with_retry` mirrors the Rust `CoreError::is_retryable` (408/429 +
5xx + transport errors) so a Harbor blip during a rolling redeploy doesn't abort
an operation whose blob requests would have retried. `base_delay=0.0` keeps these
unit tests instant.
"""
from __future__ import annotations

import httpx

from hippius_hub import _http


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
