"""Adversarial coverage for the resumable chunked-PATCH blob upload.

`uploader.rs` uploads a blob in `HIPPIUS_UPLOAD_CHUNK_SIZE`-byte chunks via OCI
`PATCH`, and on any transient failure `GET`s the registry's committed offset and
resumes from there instead of re-sending the whole blob. These tests drive the
real Rust code (`upload_blob_native`) against a stateful mock registry that
*reassembles* every PATCHed byte and verifies the final digest — so any resume
bug that skips, duplicates, or re-orders bytes fails the upload, not just a
counter.

Deliberately failure-first (no happy-path-only cases): mid-chunk disconnect,
server-side partial commit, dropped-response-after-commit (the 0.5.2 bug class),
session-gone restart, PATCH-unsupported fallback, permanent 4xx fail-fast,
bounded stall (no infinite loop / hang), 416 resync, and close-PUT retry.
"""
from __future__ import annotations

import hashlib
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

try:
    from hippius_hub.hippius_core import upload_blob_native
except ImportError:  # pragma: no cover
    pytest.skip("hippius_core extension not built; run `maturin develop`", allow_module_level=True)


CHUNK = 4096  # forced via HIPPIUS_UPLOAD_CHUNK_SIZE so a small payload spans many chunks


def _payload(n_chunks: int) -> bytes:
    # Deterministic, position-dependent bytes so a mis-ordered/duplicated resume
    # changes the digest (unlike all-zeros, which would hide such a bug).
    return bytes((i * 31 + 7) & 0xFF for i in range(n_chunks * CHUNK + 123))


def _digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


class _State:
    """Per-server mock registry state + failure-injection knobs."""

    def __init__(self) -> None:
        self.sessions: dict[str, bytearray] = {}
        self.post_count = 0
        self.patch_count = 0
        self.get_count = 0
        self.close_count = 0
        self.monolithic_count = 0
        self.patch_offsets: list[int] = []
        self.patch_lengths: list[int] = []  # committed body size per PATCH
        # patch_script[i] drives the (i+1)-th PATCH; "ok" once exhausted.
        # entries: "ok" | "503"|"403"|"405"|"416" | "drop" | "commit_drop" | ("partial", n)
        self.patch_script: list = []
        self.get_404 = False
        self.close_503_remaining = 0
        self.final_content: bytes = b""
        # OCI-Chunk-Min-Length advertised on POST (0 = don't advertise).
        self.min_chunk_length = 0
        # When True the mock rotates each session's `_state` token per PATCH and
        # REJECTS a request carrying a stale one — so a client that stopped
        # following the `Location` header end-to-end fails the upload.
        self.enforce_location = False
        self.current_state: dict[str, str] = {}


def _extract_uuid(path: str) -> str:
    return path.split("?", 1)[0].rstrip("/").split("/")[-1]


def _query_param(path: str, key: str) -> str:
    query = path.split("?", 1)[1] if "?" in path else ""
    return next((kv[len(key) + 1:] for kv in query.split("&") if kv.startswith(key + "=")), "")


def _read_chunked(rfile) -> bytes:
    out = bytearray()
    while True:
        line = rfile.readline().strip()
        if not line:
            continue
        size = int(line.split(b";")[0], 16)
        if size == 0:
            rfile.readline()  # trailing CRLF
            break
        out += rfile.read(size)
        rfile.readline()  # CRLF after each chunk
    return bytes(out)


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    @property
    def st(self) -> _State:
        return self.server.state  # type: ignore[attr-defined]

    def _empty(self, code: int) -> None:
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self) -> None:
        self.st.post_count += 1
        uuid = f"sess{self.st.post_count}"
        self.st.sessions[uuid] = bytearray()
        self.st.current_state[uuid] = "init"
        self.send_response(202)
        self.send_header("Location", f"/v2/foo/bar/blobs/uploads/{uuid}?_state=init")
        self.send_header("Range", "0-0")  # empty session marker
        if self.st.min_chunk_length:
            self.send_header("OCI-Chunk-Min-Length", str(self.st.min_chunk_length))
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_PATCH(self) -> None:
        self.st.patch_count += 1
        idx = self.st.patch_count - 1
        uuid = _extract_uuid(self.path)
        sess = self.st.sessions[uuid]
        cr = self.headers.get("Content-Range", "")
        offset = int(cr.split("-", 1)[0]) if "-" in cr else 0
        self.st.patch_offsets.append(offset)
        # The resumable PATCH streams a framed body → Transfer-Encoding: chunked
        # (no Content-Length). Read whichever framing the client used so the
        # reassembly stays byte-exact (Harbor accepts the chunked-TE PATCH).
        te = self.headers.get("Transfer-Encoding", "").lower()
        if "chunked" in te:
            body = _read_chunked(self.rfile)
        else:
            length = int(self.headers.get("Content-Length", "0") or "0")
            body = self.rfile.read(length) if length else b""

        # Location-following enforcement: reject a request that carries a stale
        # `_state` token (i.e. a client that stopped threading the PATCH `Location`
        # into the next request). Off by default so the other tests are unaffected.
        if self.st.enforce_location and _query_param(self.path, "_state") != self.st.current_state.get(uuid):
            self._empty(400)
            return

        action = self.st.patch_script[idx] if idx < len(self.st.patch_script) else "ok"

        if action == "drop":  # disconnect, nothing committed, no response
            self.close_connection = True
            return
        if action == "commit_drop":  # bytes committed but response lost
            if offset == len(sess):
                sess.extend(body)
            self.close_connection = True
            return
        if isinstance(action, tuple) and action[0] == "partial":
            if offset == len(sess):
                sess.extend(body[: action[1]])  # server committed a prefix, then failed
            self._empty(503)
            return
        if action in ("503", "403", "405", "416"):
            self._empty(int(action))
            return

        # "ok": require contiguous offset (out-of-order => 416, like a real registry)
        if offset != len(sess):
            self._empty(416)
            return
        sess.extend(body)
        self.st.patch_lengths.append(len(body))
        state = f"p{self.st.patch_count}"
        self.st.current_state[uuid] = state
        self.send_response(202)
        self.send_header("Location", f"/v2/foo/bar/blobs/uploads/{uuid}?_state={state}")
        self.send_header("Range", f"0-{len(sess) - 1}")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        self.st.get_count += 1
        if self.st.get_404:
            self._empty(404)
            return
        if self.st.enforce_location:
            uuid = _extract_uuid(self.path)
            if _query_param(self.path, "_state") != self.st.current_state.get(uuid):
                self._empty(400)
                return
        committed = len(self.st.sessions.get(_extract_uuid(self.path), b""))
        self.send_response(204)
        self.send_header("Range", f"0-{committed - 1}" if committed > 0 else "0-0")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_PUT(self) -> None:
        uuid = _extract_uuid(self.path)
        query = self.path.split("?", 1)[1] if "?" in self.path else ""
        digest = next((kv[len("digest="):] for kv in query.split("&") if kv.startswith("digest=")), "")
        te = self.headers.get("Transfer-Encoding", "").lower()

        if "chunked" in te:  # monolithic fallback — body is the whole file
            content = _read_chunked(self.rfile)
            self.st.monolithic_count += 1
        else:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length:
                self.rfile.read(length)
            self.st.close_count += 1
            if self.st.close_503_remaining > 0:  # transient close blip
                self.st.close_503_remaining -= 1
                self._empty(503)
                return
            content = bytes(self.st.sessions.get(uuid, b""))

        self.st.final_content = content
        ok = _digest(content) == digest
        self.send_response(201 if ok else 400)
        self.send_header("Docker-Content-Digest", digest)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *_a, **_k) -> None:  # silence
        pass


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def registry(monkeypatch):
    """A mock OCI upload endpoint with a small forced chunk size.

    Yields `(uploads_url, state)`; the test configures `state` (failure script)
    before calling `upload_blob_native`.
    """
    monkeypatch.setenv("HIPPIUS_UPLOAD_CHUNK_SIZE", str(CHUNK))
    port = _free_port()
    server = ThreadingHTTPServer(("127.0.0.1", port), _Handler)
    server.state = _State()  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/v2/foo/bar/blobs/uploads/", server.state
    finally:
        server.shutdown()
        thread.join(timeout=5)


def _upload(registry, data: bytes):
    uploads_url, _ = registry
    import tempfile
    import os

    fd, p = tempfile.mkstemp(suffix=".bin")
    os.write(fd, data)
    os.close(fd)
    try:
        upload_blob_native(uploads_url=uploads_url, path=p, digest=_digest(data), auth_token=None)
    finally:
        os.unlink(p)


# --------------------------------------------------------------------------- #

def test_multichunk_reassembles_byte_exact(registry):
    """Baseline correctness: many chunks reassemble to the exact bytes.

    Not happy-path fluff — it pins that the PATCH loop sends contiguous,
    non-overlapping ranges. The mock verifies the digest, so a duplicated or
    dropped chunk would 400 the close and fail the upload.
    """
    _, st = registry
    data = _payload(6)
    _upload(registry, data)
    assert st.post_count == 1
    assert st.patch_count == 7  # ceil(6*4096+123 / 4096) = 7 chunks
    assert st.final_content == data


def test_transient_503_resends_only_the_failed_chunk(registry):
    """A 503 on chunk 3 → GET offset → re-send just that chunk, not the file."""
    _, st = registry
    st.patch_script = ["ok", "ok", "503"]  # 3rd PATCH fails transiently
    data = _payload(5)
    _upload(registry, data)
    assert st.final_content == data
    assert st.post_count == 1  # same session (no fresh restart)
    assert st.get_count >= 1  # resumed via a status GET
    # exactly one extra PATCH vs the clean run (the re-sent chunk 3)
    assert st.patch_count == 6 + 1


def test_server_partial_commit_resume_skips_committed_bytes(registry):
    """Server commits a prefix of chunk 3 then fails → resume skips those bytes."""
    _, st = registry
    st.patch_script = ["ok", "ok", ("partial", 1000)]  # commit 1000 of the 4096
    data = _payload(4)
    _upload(registry, data)
    assert st.final_content == data
    assert st.post_count == 1
    # the re-send must start at the partial offset (2 full chunks + 1000), not
    # re-send the whole chunk 3 from 8192.
    assert (2 * CHUNK + 1000) in st.patch_offsets


def test_commit_then_dropped_response_does_not_resend(registry):
    """Bytes committed but the response is lost (the 0.5.2 bug class).

    The client sees a transport error, GETs the offset (chunk fully committed),
    and continues — it must NOT re-send the committed chunk (which a naive retry
    would, tripping the wrong-offset failure this whole line of work fixes).
    """
    _, st = registry
    st.patch_script = ["ok", "commit_drop"]  # chunk 2 commits, connection drops
    data = _payload(4)
    _upload(registry, data)
    assert st.final_content == data
    assert st.post_count == 1
    # chunk 2's range (4096) must appear exactly once — no duplicate re-send.
    assert st.patch_offsets.count(CHUNK) == 1


def test_mid_chunk_disconnect_resumes(registry):
    """Connection dropped mid-chunk (nothing committed) → GET → re-send chunk."""
    _, st = registry
    st.patch_script = ["ok", "drop"]  # chunk 2 disconnects with no commit
    data = _payload(3)
    _upload(registry, data)
    assert st.final_content == data
    assert st.post_count == 1
    assert st.patch_offsets.count(CHUNK) == 2  # chunk 2 sent twice (drop, then ok)


def test_session_gone_restarts_fresh(registry):
    """PATCH fails and the session is gone (GET 404) → fresh POST from 0."""
    _, st = registry
    st.patch_script = ["ok", "503"]
    st.get_404 = True  # the resume GET reports the session vanished
    data = _payload(3)
    _upload(registry, data)
    assert st.final_content == data
    assert st.post_count == 2  # restarted with a brand-new session


def test_patch_unsupported_falls_back_to_monolithic(registry):
    """A registry that rejects PATCH (405) on chunk 1 → monolithic PUT."""
    _, st = registry
    st.patch_script = ["405"]
    data = _payload(3)
    _upload(registry, data)
    assert st.final_content == data
    assert st.monolithic_count == 1
    assert st.close_count == 0  # no chunked close — the whole thing went as one PUT
    assert st.patch_count == 1  # gave up on PATCH after the first 405


def test_permanent_403_fails_fast(registry):
    """A permanent 4xx on PATCH is not retried and does not loop."""
    _, st = registry
    st.patch_script = ["403"] * 50  # always 403
    with pytest.raises(Exception):
        _upload(registry, _payload(3))
    # 403 is not retryable: one session, one PATCH, then surfaced.
    assert st.post_count == 1
    assert st.patch_count == 1


def test_stall_no_progress_gives_up_bounded(registry):
    """A registry that always 503s with no progress must terminate, not hang."""
    _, st = registry
    st.patch_script = ["503"] * 500  # never makes progress
    with pytest.raises(Exception):
        _upload(registry, _payload(2))
    # Bounded: (outer restarts) x (intra-session stall budget), never unbounded.
    assert st.post_count <= 4  # UPLOAD_MAX_RETRIES + 1 fresh sessions
    assert st.patch_count <= 40  # comfortably above the real bound, well below "hang"


def test_416_out_of_order_resyncs_then_completes(registry):
    """A 416 on a chunk → GET offset → re-send from there → completes."""
    _, st = registry
    st.patch_script = ["ok", "416"]  # chunk 2 rejected once as out-of-order
    data = _payload(3)
    _upload(registry, data)
    assert st.final_content == data
    assert st.post_count == 1
    assert st.get_count >= 1


def test_close_put_retries_on_transient_without_reupload(registry):
    """A transient 503 on the finalising PUT is retried in place, not restarted."""
    _, st = registry
    st.close_503_remaining = 1  # first close blips, second succeeds
    data = _payload(4)
    _upload(registry, data)
    assert st.final_content == data
    assert st.post_count == 1
    assert st.close_count == 2  # closed twice (one 503 + one 201)
    assert st.patch_count == 5  # ceil(4*4096+123/4096)=5 — bytes NOT re-uploaded


def test_empty_blob_uploads_via_close_only(registry):
    """A 0-byte blob: the `while offset < size` loop is skipped entirely, so there
    is NO PATCH — just POST + the finalising `PUT ?digest=<empty-sha256>`. This is
    the canonical OCI monolithic empty-blob close and must succeed."""
    _, st = registry
    _upload(registry, b"")
    assert st.final_content == b""
    assert st.post_count == 1
    assert st.patch_count == 0  # nothing to PATCH
    assert st.close_count == 1  # closed once, empty digest accepted


def test_client_follows_rotating_location(registry):
    """Each PATCH hands back a fresh `_state` in `Location`; the client must thread
    it into the next request. With enforcement on, a client that stopped following
    the `Location` header would send a stale `_state` and get rejected — so this
    upload only completes because the client resolves and reuses each new URL."""
    _, st = registry
    st.enforce_location = True
    data = _payload(4)
    _upload(registry, data)
    assert st.final_content == data
    assert st.post_count == 1  # no rejection → no fresh-session restart
    assert st.patch_count == 5


def test_honors_chunk_min_length(registry):
    """The registry advertises `OCI-Chunk-Min-Length` larger than the configured
    chunk size; the client must clamp UP so every non-final PATCH meets the minimum
    (the final short chunk is spec-exempt)."""
    _, st = registry
    st.min_chunk_length = 2 * CHUNK  # 8192 > the 4096 env chunk size
    data = _payload(6)  # 6*4096+123 = 24699 bytes → 8192,8192,8192,123
    _upload(registry, data)
    assert st.final_content == data
    assert st.post_count == 1
    # Non-final PATCHes must be >= the advertised minimum; only the last may be
    # smaller. With the clamp they are exactly 8192.
    assert st.patch_lengths[:-1] == [2 * CHUNK] * (len(st.patch_lengths) - 1)
    assert all(n >= st.min_chunk_length for n in st.patch_lengths[:-1])
    assert st.patch_lengths[-1] == 123
