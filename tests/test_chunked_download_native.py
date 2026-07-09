"""End-to-end test for the native chunked-artifact pull (`download_chunks_native`).

Exercises the real Rust extension against a real localhost HTTP server serving
content-addressed chunk blobs — respx can't intercept Rust's reqwest, so a
socket-bound server is the only way to put real bytes on the wire. Covers the
guarantees the plan promises: parallel assembly in file order, always-on
per-chunk digest verification, the whole-file verify pass, and loud failure on
corrupt / short chunks (never a silently-wrong file).
"""
from __future__ import annotations

import hashlib
import os
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

try:
    from hippius_hub.hippius_core import download_chunks_native
except ImportError:
    pytest.skip(
        "hippius_core extension not built; run `maturin develop`",
        allow_module_level=True,
    )


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


class _BlobServer(HTTPServer):
    """HTTPServer carrying a `blobs` dict (request path -> exact bytes to serve)."""

    blobs: dict


class _BlobHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = self.server.blobs.get(self.path)
        if body is None:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args, **_kwargs):
        pass


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def blob_server():
    """Start a localhost server for a caller-supplied {path: bytes} map.

    Returns a factory: `base_url = blob_server(blobs)`. One server per test,
    torn down on exit.
    """
    servers = []

    def _start(blobs: dict) -> str:
        port = _free_port()
        server = _BlobServer(("127.0.0.1", port), _BlobHandler)
        server.blobs = blobs
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append((server, thread))
        return f"http://127.0.0.1:{port}"

    yield _start
    for server, thread in servers:
        server.shutdown()
        thread.join(timeout=5)


def _chunk_urls(base: str, repo: str, digests: list) -> list:
    return [f"{base}/v2/{repo}/blobs/sha256:{d}" for d in digests]


def test_assembles_chunks_in_order_and_verifies_whole_file(blob_server, tmp_path):
    repo = "acme/model"
    # Varied sizes on purpose — content-defined chunks are not uniform.
    chunks = [os.urandom(1000), os.urandom(1500), os.urandom(777)]
    digests = [_sha(c) for c in chunks]
    file_bytes = b"".join(chunks)
    file_hex = _sha(file_bytes)
    blobs = {f"/v2/{repo}/blobs/sha256:{d}": c for d, c in zip(digests, chunks)}
    base = blob_server(blobs)
    dest = tmp_path / "out.bin"

    got = download_chunks_native(
        urls=_chunk_urls(base, repo, digests),
        chunk_digests=digests,
        chunk_sizes=[len(c) for c in chunks],
        dest_path=str(dest),
        file_digest=file_hex,
        auth_token=None,
        max_concurrent=8,
    )

    assert got == file_hex
    assert dest.read_bytes() == file_bytes  # order + boundaries exact


def test_skipping_whole_file_verify_returns_none_but_still_assembles(blob_server, tmp_path):
    repo = "r"
    chunks = [os.urandom(600), os.urandom(600)]
    digests = [_sha(c) for c in chunks]
    blobs = {f"/v2/{repo}/blobs/sha256:{d}": c for d, c in zip(digests, chunks)}
    base = blob_server(blobs)
    dest = tmp_path / "out.bin"

    got = download_chunks_native(
        urls=_chunk_urls(base, repo, digests),
        chunk_digests=digests,
        chunk_sizes=[len(c) for c in chunks],
        dest_path=str(dest),
        file_digest=None,  # skip whole-file pass
        auth_token=None,
        max_concurrent=4,
    )

    assert got is None
    # Per-chunk verification still ran, so the bytes are correct.
    assert dest.read_bytes() == b"".join(chunks)


def test_corrupt_chunk_fails_loudly(blob_server, tmp_path):
    repo = "r"
    chunks = [os.urandom(500), os.urandom(500)]
    digests = [_sha(c) for c in chunks]
    # Serve wrong bytes (same length) for the second chunk: its digest won't match.
    served = {digests[0]: chunks[0], digests[1]: os.urandom(500)}
    blobs = {f"/v2/{repo}/blobs/sha256:{d}": served[d] for d in digests}
    base = blob_server(blobs)

    with pytest.raises(Exception) as exc:
        download_chunks_native(
            urls=_chunk_urls(base, repo, digests),
            chunk_digests=digests,
            chunk_sizes=[len(c) for c in chunks],
            dest_path=str(tmp_path / "o.bin"),
            file_digest=None,
            auth_token=None,
            max_concurrent=4,
        )
    msg = str(exc.value).lower()
    assert "integrity" in msg and "sha256" in msg


def test_short_chunk_fails_loudly(blob_server, tmp_path):
    repo = "r"
    payload = os.urandom(500)
    d = _sha(payload)
    # Serve only 100 of the declared 500 bytes — a truncated blob.
    base = blob_server({f"/v2/{repo}/blobs/sha256:{d}": payload[:100]})

    with pytest.raises(Exception) as exc:
        download_chunks_native(
            urls=_chunk_urls(base, repo, [d]),
            chunk_digests=[d],
            chunk_sizes=[500],
            dest_path=str(tmp_path / "o.bin"),
            file_digest=None,
            auth_token=None,
            max_concurrent=1,
        )
    assert "integrity" in str(exc.value).lower()


def test_whole_file_digest_mismatch_fails_loudly(blob_server, tmp_path):
    repo = "r"
    chunks = [os.urandom(300), os.urandom(300)]
    digests = [_sha(c) for c in chunks]
    blobs = {f"/v2/{repo}/blobs/sha256:{d}": c for d, c in zip(digests, chunks)}
    base = blob_server(blobs)

    with pytest.raises(Exception) as exc:
        download_chunks_native(
            urls=_chunk_urls(base, repo, digests),
            chunk_digests=digests,
            chunk_sizes=[len(c) for c in chunks],
            dest_path=str(tmp_path / "o.bin"),
            file_digest=_sha(b"a different file"),  # chunks fine, whole-file wrong
            auth_token=None,
            max_concurrent=2,
        )
    assert "assembled file" in str(exc.value).lower()


def test_mismatched_array_lengths_rejected(tmp_path):
    with pytest.raises(Exception) as exc:
        download_chunks_native(
            urls=["http://127.0.0.1:1/v2/r/blobs/sha256:x"],
            chunk_digests=["a", "b"],  # length 2 != urls length 1
            chunk_sizes=[1],
            dest_path=str(tmp_path / "o.bin"),
            file_digest=None,
            auth_token=None,
            max_concurrent=1,
        )
    assert "equal length" in str(exc.value)
