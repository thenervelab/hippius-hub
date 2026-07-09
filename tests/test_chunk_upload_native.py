"""End-to-end native data-plane round-trip: chunk → range-upload → pull → reassemble.

Drives the real Rust extension on both ends against a real content-addressed
store server: `chunk_and_hash_native` splits a file, `upload_blob_range_native`
pushes each chunk's byte range, and `download_chunks_native` pulls them back and
reassembles. Proves the three native primitives compose into the exact original
bytes — the guarantee the OCI manifest layer sits on top of.
"""
from __future__ import annotations

import hashlib
import os
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

try:
    from hippius_hub.hippius_core import (
        chunk_and_hash_native,
        download_chunks_native,
        upload_blob_range_native,
    )
except ImportError:
    pytest.skip(
        "hippius_core extension not built; run `maturin develop`",
        allow_module_level=True,
    )


def _read_body(rfile, headers) -> bytes:
    """Read a request body, de-framing chunked Transfer-Encoding (which the range
    upload uses — it streams without a Content-Length)."""
    if headers.get("Transfer-Encoding", "").lower() == "chunked":
        data = b""
        while True:
            size = int(rfile.readline().strip(), 16)
            if size == 0:
                rfile.readline()  # trailing CRLF
                break
            data += rfile.read(size)
            rfile.readline()  # CRLF after each chunk
        return data
    return rfile.read(int(headers.get("Content-Length") or 0))


class _Store(HTTPServer):
    blobs: dict


class _StoreHandler(BaseHTTPRequestHandler):
    def do_PUT(self):
        body = _read_body(self.rfile, self.headers)
        digest = hashlib.sha256(body).hexdigest()
        self.server.blobs[digest] = body  # content-addressed, like Harbor
        self.send_response(201)
        self.send_header("Docker-Content-Digest", f"sha256:{digest}")
        self.end_headers()

    def do_GET(self):
        digest = self.path.rsplit("/", 1)[-1].replace("sha256:", "")
        body = self.server.blobs.get(digest)
        if body is None:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_a, **_k):
        pass


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def store_server():
    servers = []

    def _start() -> str:
        port = _free_port()
        server = _Store(("127.0.0.1", port), _StoreHandler)
        server.blobs = {}
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append((server, thread))
        return f"http://127.0.0.1:{port}"

    yield _start
    for server, thread in servers:
        server.shutdown()
        thread.join(timeout=5)


# avg 1024 → min 256, max 4096: small enough that a 20 KB file yields many chunks.
_AVG = 1024


def test_chunk_upload_download_roundtrip(store_server, tmp_path):
    data = os.urandom(20_000)
    src = tmp_path / "in.bin"
    src.write_bytes(data)

    whole, metas = chunk_and_hash_native(str(src), _AVG)
    assert whole == hashlib.sha256(data).hexdigest()
    assert sum(size for _h, _off, size in metas) == len(data)  # chunks tile the file
    assert len(metas) > 1  # actually exercised the multi-chunk path

    base = store_server()
    # Push each chunk's byte range as its own content-addressed blob.
    for chunk_hex, offset, size in metas:
        upload_blob_range_native(
            url=f"{base}/upload", path=str(src), offset=offset, length=size, auth_token=None
        )

    # Every chunk landed under its own digest (range PUT sent the right bytes).
    out = tmp_path / "out.bin"
    got = download_chunks_native(
        urls=[f"{base}/blob/{h}" for h, _o, _s in metas],
        chunk_digests=[h for h, _o, _s in metas],
        chunk_sizes=[s for _h, _o, s in metas],
        dest_path=str(out),
        file_digest=whole,
        auth_token=None,
        max_concurrent=8,
    )
    assert got == whole
    assert out.read_bytes() == data  # reassembled bytes are exactly the original


def test_range_upload_sends_exact_range(store_server, tmp_path):
    # A single mid-file range must upload exactly those bytes — not the whole file,
    # not a shifted window.
    data = os.urandom(5000)
    src = tmp_path / "in.bin"
    src.write_bytes(data)
    base = store_server()

    upload_blob_range_native(url=f"{base}/upload", path=str(src), offset=1000, length=2000, auth_token=None)

    # The store is content-addressed, so fetching by the slice's digest confirms
    # the range PUT sent exactly bytes [1000:3000] — nothing more, nothing less.
    import httpx

    expected = data[1000:3000]
    resp = httpx.get(f"{base}/blob/{hashlib.sha256(expected).hexdigest()}")
    assert resp.status_code == 200
    assert resp.content == expected
