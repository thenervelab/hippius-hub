"""End-to-end test for audit D4: AbortHandle cancels in-flight chunk requests.

`chunked_downloader.rs` (lines 144-152, 192-213) collects AbortHandles
on every spawn and calls `.abort()` on every survivor when any chunk
errors. Without that, the failed download_file_native returns Err but
the spawned tasks continue writing to the destination file in the
background — a connection leak and silent disk-write storm.

The test design is observational rather than reflective: a real
localhost HTTP server tracks how many slow-chunk handlers had their
TCP connection dropped mid-response (BrokenPipeError on write) vs.
completed cleanly. With abort firing, the slow chunks are aborted at
their next await point in reqwest's stream; the server detects the
broken connection when it tries to flush the response body.

A regression that dropped the .abort() call (e.g. reverting to
`buffer_unordered + early return` from the pre-D4 era) would leave
those background tasks running. The server would then complete every
slow-chunk response, and `responses_completed_without_disconnect` would
match `requests_started`.
"""
from __future__ import annotations

import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

try:
    from hippius_hub.hippius_core import download_file_native
except ImportError:
    pytest.skip(
        "hippius_core extension not built; run `maturin develop`",
        allow_module_level=True,
    )


# Big enough to force multiple Range chunks at chunk_size=256.
PAYLOAD_LEN = 4096
PAYLOAD = b"A" * PAYLOAD_LEN


class _AbortObservingHandler(BaseHTTPRequestHandler):
    """Returns 401 to the first chunk (forcing the abort path) and a SLOW
    206 to every other chunk. Tracks how many slow handlers got their
    connection dropped (BrokenPipeError on write) vs. completed.

    Class attributes so the fixture and tests can both observe them.
    The shape is:
        - slow_started      = N: server began processing N slow-chunk requests
        - slow_completed    = M: server successfully wrote N - cancelled responses
        - slow_cancelled    = N - M: in-flight chunks aborted mid-flight
    """
    slow_started = 0
    slow_completed = 0
    slow_cancelled = 0
    _lock = threading.Lock()
    SLOW_SLEEP_SEC = 6.0  # well beyond any reasonable abort-propagation latency

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", str(PAYLOAD_LEN))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()

    def do_GET(self):
        cls = type(self)
        range_header = self.headers.get("Range", "")
        # Parse "bytes=START-END" — only START matters for routing.
        if range_header.startswith("bytes="):
            start_str = range_header[len("bytes="):].split("-", 1)[0]
            chunk_start = int(start_str) if start_str.isdigit() else -1
        else:
            chunk_start = -1

        if chunk_start == 0:
            # Chunk 0 fails fast — forces the abort path in Rust.
            self.send_response(401)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        with cls._lock:
            cls.slow_started += 1
        time.sleep(cls.SLOW_SLEEP_SEC)
        try:
            self.send_response(206)
            self.send_header("Content-Range",
                             f"bytes {chunk_start}-{chunk_start + 255}/{PAYLOAD_LEN}")
            self.send_header("Content-Length", "256")
            self.end_headers()
            self.wfile.write(PAYLOAD[chunk_start:chunk_start + 256])
            self.wfile.flush()
            with cls._lock:
                cls.slow_completed += 1
        except (BrokenPipeError, ConnectionResetError, OSError):
            # The Rust side aborted this chunk; the connection went away
            # during the slow window. THIS is the observable for D4.
            with cls._lock:
                cls.slow_cancelled += 1

    def log_message(self, *_args, **_kwargs):
        pass


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def abort_observing_server():
    _AbortObservingHandler.slow_started = 0
    _AbortObservingHandler.slow_completed = 0
    _AbortObservingHandler.slow_cancelled = 0
    port = _free_port()
    # ThreadingHTTPServer would be cleaner but stdlib HTTPServer is
    # single-threaded — we need parallelism for the slow handlers, so
    # use ThreadingHTTPServer from http.server.
    from http.server import ThreadingHTTPServer
    server = ThreadingHTTPServer(("127.0.0.1", port), _AbortObservingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/blob"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_chunk_error_aborts_inflight_siblings(abort_observing_server, tmp_path):
    """Chunk 0's 401 must abort the other in-flight slow chunks.

    Observable: after download_file_native errors, slow_cancelled >= 1
    (at least one in-flight chunk had its connection severed mid-sleep).

    Without abort firing, every slow handler would wake from its 2s
    sleep, write the response cleanly, and slow_completed would equal
    slow_started. The test catches that regression by requiring at
    least one chunk to be cancelled before its sleep finished.

    The exact count depends on tokio task scheduling — under high
    contention some tasks may not have entered their reqwest await
    before the abort fires, in which case the server never sees that
    request at all. The minimum-of-one assertion is the safest pin.
    """
    dest = tmp_path / "blob.bin"

    start = time.time()
    with pytest.raises(Exception):
        download_file_native(
            url=abort_observing_server,
            dest_path=str(dest),
            auth_token=None,
            chunk_size=256,  # 16 chunks total
            verify_hash=False,
        )
    elapsed = time.time() - start

    # The function must return quickly — well below the
    # SLOW_SLEEP_SEC=2.0 floor. Even if abort had been entirely broken,
    # the `return Err(...)` on first chunk error would land before the
    # slow chunks finish, so this isn't itself a D4-specific signal —
    # but a regression that introduced a blocking `awaiting all chunks`
    # path before propagating the error would surface here.
    assert elapsed < 4.0, (
        f"download_file_native took {elapsed:.1f}s, expected <4s — "
        "a regression that blocked on slow chunks before propagating "
        "the chunk-0 error would surface here."
    )

    # Wait long enough that any *non-aborted* slow handlers would have
    # completed. SLOW_SLEEP_SEC * 1.5 + buffer = ~9s here.
    time.sleep(_AbortObservingHandler.SLOW_SLEEP_SEC + 2.0)

    started = _AbortObservingHandler.slow_started
    completed = _AbortObservingHandler.slow_completed
    cancelled = _AbortObservingHandler.slow_cancelled

    # At least one slow chunk must have been ABORTED — that's the
    # behavioral pin for D4. Otherwise the test passed by accident
    # (tokio happened to schedule chunk 0 first and the others never
    # got far enough to reach reqwest's await).
    assert started >= 1, (
        f"server saw no slow-chunk requests at all (started={started}); "
        "test cannot prove abort behavior — adjust SLOW_SLEEP_SEC or chunk count"
    )
    assert cancelled >= 1, (
        f"D4 regression: no slow chunks were aborted mid-flight. "
        f"started={started}, completed={completed}, cancelled={cancelled}. "
        "Without AbortHandle.abort() firing, every started chunk would "
        "complete cleanly (cancelled=0)."
    )
