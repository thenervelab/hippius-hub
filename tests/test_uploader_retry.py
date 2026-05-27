"""End-to-end test for audit U3: uploader retries transient 5xx via Rust.

`uploader.rs:upload_blob_async` wraps `try_upload_blob_once` in an
exponential-backoff retry loop. The classifier (`CoreError::is_retryable`)
decides whether a failed PUT is permanent (4xx) or transient (5xx).

Rust unit tests cover `is_retryable` in isolation. This file is the
integration counterpart: a real localhost HTTP server that responds
503 on the first PUT and 201 on the second, then `upload_blob_native`
called through the real pyo3 extension. The behavioral pins are:

  - upload_blob_native returns successfully (no Python-side exception)
  - The server saw exactly 2 PUT requests (one retry happened)

respx isn't a fit — it patches httpx, not the Rust reqwest client. A
real socket-bound HTTP server is what puts 5xx bytes on the wire that
Rust will read.
"""
from __future__ import annotations

import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

try:
    from hippius_hub.hippius_core import upload_blob_native
except ImportError:
    pytest.skip(
        "hippius_core extension not built; run `maturin develop`",
        allow_module_level=True,
    )


PAYLOAD = b"upload-me-please\n"


class _RetryHandler(BaseHTTPRequestHandler):
    """Replies 503 then 201 to PUT — simulates a Harbor restart mid-upload.

    The class attribute `attempt_count` is mutated by every request; the
    fixture resets it to 0 before each test. Using a class attribute (not
    instance) because HTTPServer creates a new handler per request.
    """
    attempt_count = 0

    def do_PUT(self):
        cls = type(self)
        cls.attempt_count += 1
        # Drain the body so the Rust client sees a clean response cycle
        # — a 503 with an unread body can confuse some clients into
        # double-counting the request.
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length:
            self.rfile.read(length)
        if cls.attempt_count == 1:
            # First attempt: transient server error. Rust's
            # is_retryable() classifies any 5xx as retryable.
            self.send_response(503)
            self.send_header("Content-Length", "0")
            self.end_headers()
        else:
            # Retry: success. Harbor returns 201 with a digest header on
            # blob PUT; we only need the status code for is_retryable
            # accounting.
            self.send_response(201)
            self.send_header("Content-Length", "0")
            self.end_headers()

    def log_message(self, *_args, **_kwargs):
        pass  # silence per-request stderr noise


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def retry_server():
    """Spin a server that 503s once then 201s for the duration of one test."""
    _RetryHandler.attempt_count = 0
    port = _free_port()
    server = HTTPServer(("127.0.0.1", port), _RetryHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/v2/foo/bar/blobs/uploads/abc?digest=sha256:fake"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_503_then_201_succeeds_after_retry(retry_server, tmp_path):
    """upload_blob_native must transparently retry on 5xx and return Ok.

    A regression that disabled the retry (or mis-classified 5xx as
    permanent) would surface the first 503 as a terminal exception in
    Python — that's what this test pins.
    """
    payload_path = tmp_path / "blob.bin"
    payload_path.write_bytes(PAYLOAD)

    # Must not raise.
    upload_blob_native(url=retry_server, path=str(payload_path), auth_token=None)

    assert _RetryHandler.attempt_count == 2, (
        f"expected exactly 2 PUT attempts (1 503 + 1 retry), "
        f"got {_RetryHandler.attempt_count}"
    )


# ---- 4xx is permanent — must NOT retry ----


class _Permanent4xxHandler(BaseHTTPRequestHandler):
    """Always responds 403. Rust's classifier says 4xx is permanent → no
    retry → exactly one PUT, then a terminal exception in Python.
    """
    attempt_count = 0

    def do_PUT(self):
        cls = type(self)
        cls.attempt_count += 1
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length:
            self.rfile.read(length)
        self.send_response(403)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *_args, **_kwargs):
        pass


@pytest.fixture
def permanent_4xx_server():
    _Permanent4xxHandler.attempt_count = 0
    port = _free_port()
    server = HTTPServer(("127.0.0.1", port), _Permanent4xxHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/v2/foo/bar/blobs/uploads/xyz"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_403_does_not_retry(permanent_4xx_server, tmp_path):
    """A 4xx is permanent — the retry budget MUST NOT be consumed.

    Without this pin, a regression that broadened `is_retryable` to "any
    non-2xx" would silently turn every 403 into a 4-attempt loop (1.4s of
    wasted backoff before a misleading "we tried hard" error). Rust unit
    tests cover the classifier; this test pins the END-TO-END behavior:
    exactly one PUT lands on the server, then Python sees an exception.
    """
    payload_path = tmp_path / "blob.bin"
    payload_path.write_bytes(PAYLOAD)

    with pytest.raises(Exception):
        upload_blob_native(
            url=permanent_4xx_server,
            path=str(payload_path),
            auth_token=None,
        )

    assert _Permanent4xxHandler.attempt_count == 1, (
        f"4xx must not provoke retries; saw "
        f"{_Permanent4xxHandler.attempt_count} PUT attempts"
    )
