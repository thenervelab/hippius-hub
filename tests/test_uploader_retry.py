"""End-to-end test for audit U3 + L2: the plain uploader retries transient 5xx,
re-initiating a fresh OCI upload session per attempt, all in Rust.

`uploader.rs:upload_blob_async` wraps `try_upload_blob_once` in an
exponential-backoff retry loop. Since audit L2 each attempt does its OWN
POST-init + streaming PUT-with-digest (symmetry with the pack path), so a retry
never re-PUTs a session a failed attempt already consumed — the classifier
(`CoreError::is_retryable`) decides whether a failed PUT is permanent (4xx) or
transient (5xx).

Rust unit tests cover `is_retryable` in isolation. This file is the integration
counterpart: a real localhost HTTP server that answers the init POST (202 +
Location) and then 503 on the first PUT / 201 on the second, driven through the
real pyo3 extension. The behavioral pins are:

  - upload_blob_native returns successfully (no Python-side exception)
  - The server saw exactly 2 PUT attempts (one retry happened), each preceded by
    its own init POST (re-init per retry — audit L2)

respx isn't a fit — it patches httpx, not the Rust reqwest client. A real
socket-bound HTTP server is what puts 5xx bytes on the wire that Rust will read.
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
DIGEST = "sha256:fake"


class _RetryHandler(BaseHTTPRequestHandler):
    """Answers the init POST (202 + Location), then 503 then 201 to PUT —
    simulates a Harbor restart mid-upload. The retry does its own POST-init, so a
    fresh session is minted each attempt.

    The class attributes are mutated by every request; the fixture resets them to
    0 before each test. Class (not instance) attributes because HTTPServer creates
    a new handler per request.
    """
    post_count = 0
    put_count = 0

    def do_POST(self):
        cls = type(self)
        cls.post_count += 1
        # OCI blob-upload init: hand back a session Location the client PUTs to.
        self.send_response(202)
        self.send_header("Location", "/v2/foo/bar/blobs/uploads/session")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_PUT(self):
        cls = type(self)
        cls.put_count += 1
        # Drain the body so the Rust client sees a clean response cycle — a 503
        # with an unread body can confuse some clients into double-counting.
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length:
            self.rfile.read(length)
        # First PUT: transient server error (any 5xx is retryable). Retry: 201.
        self.send_response(503 if cls.put_count == 1 else 201)
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
    """Spin a server that 503s once then 201s (per-attempt init POST) for one test."""
    _RetryHandler.post_count = 0
    _RetryHandler.put_count = 0
    port = _free_port()
    server = HTTPServer(("127.0.0.1", port), _RetryHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/v2/foo/bar/blobs/uploads/"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_503_then_201_succeeds_after_retry(retry_server, tmp_path):
    """upload_blob_native must transparently retry on 5xx and return Ok.

    A regression that disabled the retry (or mis-classified 5xx as permanent)
    would surface the first 503 as a terminal exception in Python. A regression
    that re-PUT the consumed session instead of re-initiating (undoing L2) would
    show fewer POSTs than PUTs.
    """
    payload_path = tmp_path / "blob.bin"
    payload_path.write_bytes(PAYLOAD)

    # Must not raise.
    upload_blob_native(uploads_url=retry_server, path=str(payload_path), digest=DIGEST, auth_token=None)

    assert _RetryHandler.put_count == 2, (
        f"expected exactly 2 PUT attempts (1 503 + 1 retry), got {_RetryHandler.put_count}"
    )
    assert _RetryHandler.post_count == 2, (
        f"audit L2: each attempt must re-init its own session, so 2 POSTs expected, "
        f"got {_RetryHandler.post_count}"
    )


# ---- 4xx is permanent — must NOT retry ----


class _Permanent4xxHandler(BaseHTTPRequestHandler):
    """Answers the init POST, then always 403 on PUT. Rust's classifier says 4xx
    is permanent → no retry → exactly one PUT, then a terminal exception in Python.
    """
    post_count = 0
    put_count = 0

    def do_POST(self):
        cls = type(self)
        cls.post_count += 1
        self.send_response(202)
        self.send_header("Location", "/v2/foo/bar/blobs/uploads/session")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_PUT(self):
        cls = type(self)
        cls.put_count += 1
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
    _Permanent4xxHandler.post_count = 0
    _Permanent4xxHandler.put_count = 0
    port = _free_port()
    server = HTTPServer(("127.0.0.1", port), _Permanent4xxHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/v2/foo/bar/blobs/uploads/"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_403_does_not_retry(permanent_4xx_server, tmp_path):
    """A 4xx is permanent — the retry budget MUST NOT be consumed.

    Without this pin, a regression that broadened `is_retryable` to "any non-2xx"
    would silently turn every 403 into a 4-attempt loop. Rust unit tests cover the
    classifier; this test pins the END-TO-END behavior: exactly one PUT lands on
    the server, then Python sees an exception.
    """
    payload_path = tmp_path / "blob.bin"
    payload_path.write_bytes(PAYLOAD)

    with pytest.raises(Exception):
        upload_blob_native(
            uploads_url=permanent_4xx_server,
            path=str(payload_path),
            digest=DIGEST,
            auth_token=None,
        )

    assert _Permanent4xxHandler.put_count == 1, (
        f"4xx must not provoke retries; saw {_Permanent4xxHandler.put_count} PUT attempts"
    )
