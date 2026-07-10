"""End-to-end tests for the Rust blob uploader's retry + fresh-session behavior.

`uploader.rs:upload_blob_async` wraps `try_upload_blob_once` in an
exponential-backoff retry loop. `try_upload_blob_once` starts a BRAND-NEW upload
session (its own `POST /blobs/uploads/`) before streaming the PUT, so every
retry attempt gets a fresh session at offset 0.

Why fresh-per-attempt matters (the production bug this pins): re-using one
session across retries meant that when a mid-stream disconnect left the
registry's session at a nonzero offset, the retry's monolithic PUT (offset 0)
was rejected with "upload resumed at wrong offset" → 404 BLOB_UPLOAD_INVALID —
so a *recoverable* disconnect became a hard failure. (Observed live on
`0x998/albedo-qwen3.6-35b-miner-*`.)

respx isn't a fit — it patches httpx, not the Rust reqwest client. A real
socket-bound HTTP server is what puts POST/PUT bytes on the wire that Rust reads.
The server below rejects any *re-used* session with a 404, so a regression that
went back to one-session-per-blob would fail these tests, not just skip a pin.
"""
from __future__ import annotations

import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

try:
    from hippius_hub.hippius_core import upload_blob_native
except ImportError:
    pytest.skip(
        "hippius_core extension not built; run `maturin develop`",
        allow_module_level=True,
    )


PAYLOAD = b"upload-me-please\n"
DIGEST = "sha256:" + "f" * 64


class _Registry(BaseHTTPRequestHandler):
    """Mock OCI blob-upload endpoint.

    - POST /blobs/uploads/ → 202 with a UNIQUE `Location` per call (sess-N).
    - PUT  /blobs/uploads/sess-N → per-attempt status from `put_statuses`; a PUT
      to a session that was already PUT to returns 404 (the "wrong offset"
      rejection), so re-using a session is observable as a failure.

    Class attributes accumulate across requests; the fixture resets them.
    """
    post_count = 0
    put_count = 0
    put_sessions: list[str] = []
    put_statuses: list[int] = [503, 201]

    def do_POST(self):
        cls = type(self)
        cls.post_count += 1
        self.send_response(202)
        self.send_header("Location", f"/v2/foo/bar/blobs/uploads/sess-{cls.post_count}")
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()

    def do_PUT(self):
        cls = type(self)
        session = self.path.split("?", 1)[0].rsplit("/", 1)[-1]
        # Re-PUT of an already-used session = the offset-0-on-a-nonzero-session
        # bug. Reject it exactly like Harbor does so a regression is loud.
        if session in cls.put_sessions:
            status = 404
        else:
            status = cls.put_statuses[cls.put_count]
            cls.put_sessions.append(session)
            cls.put_count += 1
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()

    def log_message(self, *_args, **_kwargs):
        pass  # silence per-request stderr noise


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def registry_server(request):
    """Spin the mock registry with a given PUT-status sequence for one test."""
    _Registry.post_count = 0
    _Registry.put_count = 0
    _Registry.put_sessions = []
    _Registry.put_statuses = getattr(request, "param", [503, 201])
    port = _free_port()
    server = ThreadingHTTPServer(("127.0.0.1", port), _Registry)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/v2/foo/bar/blobs/uploads/"
    finally:
        server.shutdown()
        thread.join(timeout=5)


@pytest.mark.parametrize("registry_server", [[503, 201]], indirect=True)
def test_transient_5xx_retries_with_a_fresh_session(registry_server, tmp_path):
    """A 503 then a 201: the upload retries and succeeds — on a NEW session.

    Pins the fix: each attempt POSTs a fresh session, so the retry never re-PUTs
    a partially-filled session (which the server would 404). A regression to
    one-session-per-blob would re-PUT sess-1, get 404 (permanent), and raise.
    """
    payload = tmp_path / "blob.bin"
    payload.write_bytes(PAYLOAD)

    upload_blob_native(registry_server, DIGEST, str(payload), None)  # must not raise

    assert _Registry.post_count == 2, (
        f"each attempt must open a fresh session: expected 2 POSTs, got {_Registry.post_count}"
    )
    assert _Registry.put_count == 2, f"expected 2 PUTs (1 retry), got {_Registry.put_count}"
    assert _Registry.put_sessions == ["sess-1", "sess-2"], (
        f"retry must target a distinct session, got {_Registry.put_sessions}"
    )


@pytest.mark.parametrize("registry_server", [[403]], indirect=True)
def test_permanent_4xx_does_not_retry(registry_server, tmp_path):
    """A 403 on the PUT is permanent — exactly one session, one PUT, then raise.

    Without this pin a regression broadening `is_retryable` to "any non-2xx"
    would turn every 403 into a 4-attempt loop (~1.4 s wasted backoff).
    """
    payload = tmp_path / "blob.bin"
    payload.write_bytes(PAYLOAD)

    with pytest.raises(Exception):
        upload_blob_native(registry_server, DIGEST, str(payload), None)

    assert _Registry.post_count == 1, f"4xx must not re-init a session, got {_Registry.post_count}"
    assert _Registry.put_count == 1, f"4xx must not retry, got {_Registry.put_count} PUTs"
