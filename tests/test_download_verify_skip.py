"""End-to-end test for audit L6: verify_hash=False yields None, not "".

`download_file_native` was changed from returning `str` (with `""` as
an in-band sentinel for "skipped verify") to `Optional[str]` (`None`
for skipped, `str` for the hex digest when verify_hash=True). The
previous contract collided with any legitimate empty-string sentinel
a future caller might add, and forced every caller to know the
convention.

The Rust side handles this via `Option<String>` and pyo3 0.20+'s blanket
`IntoPy<Option<T>>` impl. test_roundtrip.py:109 (`test_verify_hash_true`)
pins the positive case end-to-end. This file adds the negative case so
a regression that re-introduced `""` as the skipped sentinel would
surface immediately.
"""
from __future__ import annotations

import hashlib
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


PAYLOAD = b"verify-skip-payload\n" * 64  # ~1 KiB


class _SimpleHandler(BaseHTTPRequestHandler):
    """HEAD returns Content-Length; GET honors Range with a 206."""

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", str(len(PAYLOAD)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()

    def do_GET(self):
        range_header = self.headers.get("Range", "")
        if not range_header.startswith("bytes="):
            self.send_response(200)
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.end_headers()
            self.wfile.write(PAYLOAD)
            return
        spec = range_header[len("bytes="):]
        start_s, end_s = spec.split("-", 1)
        start = int(start_s)
        end = int(end_s) if end_s else len(PAYLOAD) - 1
        self.send_response(206)
        self.send_header(
            "Content-Range",
            f"bytes {start}-{end}/{len(PAYLOAD)}",
        )
        self.send_header("Content-Length", str(end - start + 1))
        self.end_headers()
        self.wfile.write(PAYLOAD[start:end + 1])

    def log_message(self, *_args, **_kwargs):
        pass


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def simple_blob_server():
    port = _free_port()
    server = HTTPServer(("127.0.0.1", port), _SimpleHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/blob"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_verify_hash_false_returns_none(simple_blob_server, tmp_path):
    """A regression that reverted L6 to returning `""` would surface as
    `result == ""` rather than `result is None`. Pin `is None` so the
    in-band sentinel can never come back.
    """
    dest = tmp_path / "blob.bin"
    result = download_file_native(
        url=simple_blob_server,
        dest_path=str(dest),
        auth_token=None,
        chunk_size=256,
        verify_hash=False,
    )
    assert result is None, (
        f"verify_hash=False must return None (Optional[str]); got "
        f"{result!r} of type {type(result).__name__}. A regression to the "
        f"in-band \"\" sentinel would surface as ('' is None) == False."
    )
    # Sanity: the bytes still landed on disk regardless of verify mode.
    assert dest.read_bytes() == PAYLOAD


def test_verify_hash_true_returns_hex_digest(simple_blob_server, tmp_path):
    """Positive counter-case: verify_hash=True returns the hex digest.
    Without both sides pinned, a regression that returned None
    unconditionally (broken verify path) would pass the negative case
    above silently.
    """
    dest = tmp_path / "blob.bin"
    expected_sha = hashlib.sha256(PAYLOAD).hexdigest()

    result = download_file_native(
        url=simple_blob_server,
        dest_path=str(dest),
        auth_token=None,
        chunk_size=256,
        verify_hash=True,
    )
    assert isinstance(result, str), (
        f"verify_hash=True must return str; got {result!r} of type "
        f"{type(result).__name__}"
    )
    assert result == expected_sha, (
        f"hex digest mismatch — expected {expected_sha!r}, got {result!r}"
    )
    # No 0x prefix, no sha256: prefix — just bare 64-char lower-hex
    assert len(result) == 64
    assert result == result.lower()
