"""End-to-end test for audit D2 + L5: Rust's status discipline on a Range GET.

`chunked_downloader.rs:require_acceptable_status` rejects a 200 OK to a Range
request whose range is NOT the whole object (a MULTI-chunk download) — without
that check, a server (or buggy proxy) that silently ignored the Range header
would respond with the full body, and we'd seek to the chunk's offset and
overwrite everything past `end + 1`, producing a silently corrupt file. The one
exception (audit L5) is a single-chunk download whose range IS the whole file: a
200-with-full-body is then RFC-legal and correct, so it is accepted.

The Rust unit tests `rejects_range_ignored_200_with_diagnostic` /
`accepts_whole_file_200` cover the helper in isolation. This file is the
integration counterpart: a real localhost HTTP server, the real Rust extension
via `download_file_native`, and assertions on both the multi-chunk rejection
(with the diagnostic) and the whole-file acceptance.

respx isn't a fit here — it monkeypatches httpx, not the Rust reqwest
client. A real socket-bound `http.server.HTTPServer` is the simplest way
to put 200-not-206 bytes on the wire that Rust will read.
"""
from __future__ import annotations

import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

try:
    from hippius_hub.hippius_core import download_file_native
except ImportError:
    pytest.skip(
        "hippius_core extension not built; run `maturin develop`",
        allow_module_level=True,
    )


# ---- HTTP server fixtures ----

PAYLOAD = b"A" * 1024  # 1 KiB; large enough to force a Range request


class _PartialContentHandler(BaseHTTPRequestHandler):
    """Responds with 200-OK-and-full-body even when given a Range header.

    This is the *regression target*: the bug Rust catches at the
    `require_partial_content` gate. A real registry that did this would
    silently corrupt every multi-chunk download.
    """

    def do_HEAD(self):
        # HEAD must answer so the downloader can compute chunk_count from
        # Content-Length and decide to issue Range requests.
        self.send_response(200)
        self.send_header("Content-Length", str(len(PAYLOAD)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()

    def do_GET(self):
        # The regression: 200 OK + full body, even though the client sent
        # `Range: bytes=...`. Rust must reject this with the typed
        # "ignored Range" diagnostic.
        self.send_response(200)
        self.send_header("Content-Length", str(len(PAYLOAD)))
        self.end_headers()
        self.wfile.write(PAYLOAD)

    def log_message(self, *_args, **_kwargs):
        # Silence the default per-request stderr log; the test only cares
        # about behavior, not the server's transcript.
        pass


def _free_port() -> int:
    """Reserve a localhost port for the duration of one test.

    Uses the same trick as pytest-httpserver / pytest-localserver: bind
    to port 0, ask the kernel which port it gave us, then close — there's
    a small race before the test rebinds, but it's good enough for our
    one-server-per-test pattern.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def partial_content_violator():
    """Spin a one-request HTTP server that returns 200-not-206 to Range."""
    port = _free_port()
    server = HTTPServer(("127.0.0.1", port), _PartialContentHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/blob"
    finally:
        server.shutdown()
        thread.join(timeout=5)


# ---- Behavior test ----


def test_200_to_range_request_raises_with_diagnostic(partial_content_violator, tmp_path):
    """download_file_native must refuse a 200 OK to a Range request.

    The error message must name the byte range so operators see exactly
    which chunk hit the broken proxy/registry — a generic "download
    failed" wouldn't be actionable.
    """
    dest = tmp_path / "blob.bin"

    with pytest.raises(Exception) as exc_info:
        download_file_native(
            url=partial_content_violator,
            dest_path=str(dest),
            auth_token=None,
            # Small chunk size forces multi-chunk → Range requests.
            chunk_size=256,
            verify_hash=False,
        )

    msg = str(exc_info.value)
    # The diagnostic is what makes this catchable in production logs.
    # Either substring is acceptable; the exact wording is defined in
    # chunked_downloader.rs:380-382.
    assert "ignored Range" in msg or "200 OK instead of 206" in msg, (
        f"expected the 'ignored Range' / '200 instead of 206' diagnostic, "
        f"got: {msg!r}"
    )
    # Pin that at least one byte offset shows up so operators can grep
    # which chunk failed (the bytes=START-END portion of the diagnostic).
    assert "bytes=" in msg, (
        f"expected a 'bytes=START-END' range fragment in diagnostic, "
        f"got: {msg!r}"
    )

    # And — critically — the destination must NOT have been left in a
    # corrupt-but-superficially-complete state. mkstemp/dest may exist
    # because Rust pre-allocates the file; what matters is that no bytes
    # were silently overwritten outside the requested range. A successful
    # corrupt write would have left exactly len(PAYLOAD) bytes from the
    # FIRST chunk, then the test would have no signal. The exception is
    # the signal — we're done if it raises.


def test_200_to_range_single_chunk_whole_file_is_accepted(partial_content_violator, tmp_path):
    """Audit L5: when the request covers the WHOLE object (a single-chunk
    small-file download, chunk_size >= content_length), a `200 OK` with the full
    body is RFC 9110 §15.3.7-legal and correct — the full body written at offset 0
    IS the file — so it is accepted and written, not rejected.

    This intentionally reverses the earlier reject-all-200 behavior. The
    MULTI-chunk range-ignored 200 (where the range is NOT the whole object) is
    still rejected loudly — see `test_200_to_range_request_raises_with_diagnostic`.
    """
    dest = tmp_path / "blob.bin"

    # chunk_size > PAYLOAD → one Range request spanning the whole file, so the
    # server's 200-with-full-body is the correct whole file, not a range-ignored
    # partial. download_file_native must succeed and write it.
    download_file_native(
        url=partial_content_violator,
        dest_path=str(dest),
        auth_token=None,
        chunk_size=4096,
        verify_hash=False,
    )

    assert dest.read_bytes() == PAYLOAD
