#!/usr/bin/env python3
"""Benchmark the end-to-end chunked-v2 upload wall clock against a local registry.

Drives the REAL `hippius_hub.file_upload.upload_file` — manifest fetch, native
chunk+hash pipeline, pack uploads, pointer blob, config blob, manifest PUT —
over real loopback sockets against a minimal embedded OCI registry, so pipeline
changes (e.g. overlapping phase-1 hashing with the network) can be compared
before/after without a live registry. Companion to `bench_phase1.py`, which
isolates phase 1 alone.

What is stubbed (and nothing else):
- `call_with_oci_token_refresh` is monkeypatched to call the operation with a
  fixed token, bypassing the api.hippius.com token endpoint entirely. Every
  registry-facing request is real.
- The embedded registry implements ONLY what this client actually sends:
    POST /v2/<repo>/blobs/uploads/     -> 202 + Location (fresh session)
    PUT  <session>?digest=...          -> 201 (body drained + counted; both
                                          chunked-TE from the Rust uploader and
                                          Content-Length from httpx)
    HEAD /v2/<repo>/blobs/<digest>     -> 404 (forces every upload; the pack
                                          path never HEADs — packs are new by
                                          construction)
    GET  /v2/<repo>/manifests/<rev>    -> 404 (fresh repo, missing_ok path)
    PUT  /v2/<repo>/manifests/<rev>    -> 201 + Docker-Content-Digest
  No PATCH: the pack path uploads each pack as one monolithic PUT-with-digest
  (`try_pack_upload_once`), and the pointer/config blobs go through httpx's
  POST + monolithic PUT (`_put_blob_with_session`). Received bodies are
  discarded but COUNTED, and each run asserts total received bytes are the
  file size plus a small overhead (pointer + config + manifest) — so a client
  that silently skipped bytes fails the run rather than posting a fast time.

Environment forced by this script:
- HIPPIUS_CHUNK_THRESHOLD=1 so ANY size takes the chunked-v2 path (the default
  threshold is 256 MiB; forcing it keeps small --size-mib smoke runs on the
  same code path as the 2 GiB measurement).
Worker counts are left at production defaults (HIPPIUS_UPLOAD_WORKERS etc.).

--throttle-mibs N caps the AGGREGATE body-read rate across all connections
(default off = loopback-max). Global rather than per-socket on purpose: pack
uploads run 8-way parallel, so a per-socket cap would admit 8x the figure and
stop simulating a bandwidth-limited pipe (the recorded real-registry aggregate
is ~200-240 MiB/s). Throttled runs are where pipeline overlap shows: wall
clock approaches max(phase-1, transfer) instead of their sum.

Loopback-max (no throttle) numbers include the embedded Python registry's own
receive cost (GIL-bound ThreadingHTTPServer draining the bodies), so they are
A/B-comparable between builds but are NOT an absolute client ceiling; treat
the throttled cells as the representative ones.

The input file is deterministic (seed 0xB0, same generator as bench_phase1.py,
duplicated here so this script is self-contained on older checkouts) and
reused across runs. Each run uploads to a FRESH repo name so the process-wide
config-blob cache and registry state never let run N+1 skip work run N did.

IMPORTANT: run against a release build (`maturin develop --release`); a debug
build measures nothing meaningful.

Examples:

    # Default: 2 GiB input, 3 timed runs, loopback-max.
    python scripts/bench_e2e_upload.py

    # Simulate a ~200 MiB/s registry.
    python scripts/bench_e2e_upload.py --throttle-mibs 200 --runs 1

    # Quick smoke.
    python scripts/bench_e2e_upload.py --size-mib 64 --runs 1
"""
import argparse
import hashlib
import os
import platform
import random
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

MIB = 1024 * 1024
GEN_SEED = 0xB0  # must match bench_phase1.py so inputs are byte-identical
GEN_BLOCK = 1 * MIB
READ_BLOCK = 1 * MIB  # server-side drain granularity (also the throttle quantum)

# Force the chunked-v2 path BEFORE importing hippius_hub (resolved at call time,
# but setting it here keeps the forcing visible at the top of the script).
os.environ["HIPPIUS_CHUNK_THRESHOLD"] = "1"

try:
    import hippius_hub.file_upload as file_upload
except ImportError:
    print(
        "ERROR: hippius_hub/hippius_core is not installed. Build it with "
        "`maturin develop --release` (release matters).",
        file=sys.stderr,
    )
    sys.exit(1)


# ---- embedded registry ----

class GlobalThrottle:
    """Serialize body reads onto one simulated pipe of `rate` bytes/sec.

    Shared across every connection: each read reserves a slot on the wire and
    sleeps until its bytes 'took' their time, so aggregate ingest never exceeds
    the rate and no idle-time burst credit accrues between runs."""

    # Max banked credit. `time.sleep` overshoots by ~1-3 ms per call; if the
    # ledger clamped to `now` every time, that overshoot would compound (halving
    # the effective rate at 256 KiB frames). Letting the schedule run up to this
    # far behind `now` credits overshoot back, while an idle gap between runs
    # still cannot bank more than this much burst.
    BURST_SECS = 0.05

    def __init__(self, rate_bytes_per_sec: float):
        self.rate = rate_bytes_per_sec
        self.lock = threading.Lock()
        self.next_free = 0.0

    def consume(self, n: int) -> None:
        with self.lock:
            now = time.perf_counter()
            if self.next_free < now - self.BURST_SECS:
                self.next_free = now - self.BURST_SECS
            self.next_free += n / self.rate
            wake = self.next_free
        delay = wake - time.perf_counter()
        if delay > 0:
            time.sleep(delay)


class RegistryState:
    """Byte counters (per current run) + upload-session id source."""

    def __init__(self, throttle: GlobalThrottle | None):
        self.throttle = throttle
        self.lock = threading.Lock()
        self.session_seq = 0
        self.reset_run()

    def reset_run(self) -> None:
        with self.lock:
            self.blob_bytes = 0
            self.blob_puts = 0
            self.manifest_bytes = 0

    def next_session(self) -> str:
        with self.lock:
            self.session_seq += 1
            return f"sess{self.session_seq}"

    def count_blob(self, n: int) -> None:
        with self.lock:
            self.blob_bytes += n
            self.blob_puts += 1


class Handler(BaseHTTPRequestHandler):
    """Minimal OCI registry: only the endpoints the upload client actually hits."""

    protocol_version = "HTTP/1.1"

    @property
    def st(self) -> RegistryState:
        return self.server.state  # type: ignore[attr-defined]

    def _empty(self, code: int, headers: dict | None = None) -> None:
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _drain(self, n: int) -> None:
        """Read exactly n body bytes, discarding (throttled)."""
        remaining = n
        while remaining:
            data = self.rfile.read(min(remaining, READ_BLOCK))
            if not data:
                raise ConnectionError("client closed mid-body")
            if self.st.throttle:
                self.st.throttle.consume(len(data))
            remaining -= len(data)

    def _drain_body(self) -> int:
        """Drain the request body (chunked-TE or Content-Length); return its size."""
        if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
            total = 0
            while True:
                raw = self.rfile.readline()
                if not raw:
                    # EOF mid-stream (client vanished): bail out instead of
                    # spinning on empty reads forever.
                    raise ConnectionError("client closed mid-chunked-body")
                line = raw.strip()
                if not line:
                    continue
                size = int(line.split(b";")[0], 16)
                if size == 0:
                    self.rfile.readline()  # trailing CRLF
                    return total
                self._drain(size)
                self.rfile.readline()  # CRLF after each chunk
                total += size
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length:
            self._drain(length)
        return length

    def do_POST(self) -> None:  # blob upload init
        session = self.st.next_session()
        self._empty(202, {"Location": f"{self.path}{session}"})

    def do_PUT(self) -> None:  # blob monolithic PUT-with-digest, or manifest PUT
        if "/manifests/" in self.path:
            # Manifest PUTs carry Content-Length (httpx json=); hash the body so
            # Docker-Content-Digest is honest for CommitInfo.
            length = int(self.headers.get("Content-Length", "0") or "0")
            body = self.rfile.read(length)
            with self.st.lock:
                self.st.manifest_bytes += len(body)
            digest = f"sha256:{hashlib.sha256(body).hexdigest()}"
            self._empty(201, {"Docker-Content-Digest": digest})
            return
        self.st.count_blob(self._drain_body())
        self._empty(201)

    def do_HEAD(self) -> None:  # blob existence probe -> always missing
        self._empty(404)

    def do_GET(self) -> None:  # manifest fetch -> fresh repo
        self._empty(404)

    def log_message(self, *_a, **_k) -> None:
        pass


def start_registry(throttle_mibs: float | None):
    """Start the loopback registry; returns (server, state, base_url)."""
    throttle = GlobalThrottle(throttle_mibs * MIB) if throttle_mibs else None
    state = RegistryState(throttle)
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.state = state  # type: ignore[attr-defined]
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, state, f"http://127.0.0.1:{server.server_address[1]}"


# ---- input + driver ----

def ensure_input_file(path: Path, size_mib: int) -> None:
    """Deterministic input (same seed/logic as bench_phase1.py) — reused if present."""
    size = size_mib * MIB
    if path.exists() and path.stat().st_size == size:
        print(f"Reusing existing input file: {path} ({size_mib} MiB)")
        return
    print(f"Generating {size_mib} MiB deterministic input at {path} ...", flush=True)
    rng = random.Random(GEN_SEED)
    t0 = time.perf_counter()
    with open(path, "wb") as f:
        remaining = size
        while remaining > 0:
            f.write(rng.randbytes(min(GEN_BLOCK, remaining)))
            remaining -= min(GEN_BLOCK, remaining)
    print(f"  generated in {time.perf_counter() - t0:.1f}s")


def _stub_token_refresh(oci_repo, token, *, push, operation, endpoint=None, initial=None):
    """Replace the api.hippius.com token mint: run the operation with a fixed token."""
    return operation("bench-token")


def run_once(run_idx: int, endpoint: str, state: RegistryState, path: Path, size_mib: int):
    """One full upload_file against a fresh repo name; returns (secs, MiB/s)."""
    state.reset_run()
    t0 = time.perf_counter()
    file_upload.upload_file(
        path_or_fileobj=str(path),
        path_in_repo="bench.bin",
        repo_id=f"bench/e2e-run{run_idx}",
        endpoint=endpoint,
    )
    dt = time.perf_counter() - t0

    # Byte-count sanity: everything the client claims to have uploaded must have
    # crossed the socket. Blob bytes = file + pointer + 2-byte config; overhead
    # above the file size stays tiny (pointer ~100 B/chunk).
    size = size_mib * MIB
    received = state.blob_bytes + state.manifest_bytes
    overhead = received - size
    if not (0 < overhead <= 16 * MIB):
        raise AssertionError(
            f"byte-count sanity failed: received {received} for a {size}-byte file "
            f"(overhead {overhead}); the client skipped or duplicated bytes"
        )
    mibs = size_mib / dt
    print(
        f"  run {run_idx}: {dt:.2f}s  ({mibs:.1f} MiB/s, {state.blob_puts} blob PUTs, "
        f"+{overhead / 1024:.1f} KiB overhead)"
    )
    return dt, mibs


def cpu_brand() -> str:
    if sys.platform == "darwin":
        try:
            out = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True, text=True, timeout=5,
            )
            if out.returncode == 0 and out.stdout.strip():
                return out.stdout.strip()
        except (OSError, subprocess.SubprocessError):
            pass
    return platform.processor() or "unknown"


def parse_args():
    p = argparse.ArgumentParser(
        description="Benchmark end-to-end chunked-v2 upload wall clock (local OCI registry).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("\n", 1)[1] if __doc__ else None,
    )
    p.add_argument("--size-mib", type=int, default=2048,
                   help="Input file size in MiB (default: 2048)")
    p.add_argument("--runs", type=int, default=3,
                   help="Number of timed runs (default: 3)")
    p.add_argument("--throttle-mibs", type=float, default=None,
                   help="Cap aggregate registry ingest at N MiB/s (default: off = loopback-max)")
    p.add_argument("--file", type=Path, default=None,
                   help="Input file path (default: <tempdir>/hippius-bench-e2e-<size>mib.bin); "
                        "reused if it already exists with the right size")
    args = p.parse_args()
    if args.size_mib < 1:
        p.error("--size-mib must be >= 1")
    if args.runs < 1:
        p.error("--runs must be >= 1")
    if args.throttle_mibs is not None and args.throttle_mibs <= 0:
        p.error("--throttle-mibs must be > 0")
    if args.file is None:
        args.file = Path(tempfile.gettempdir()) / f"hippius-bench-e2e-{args.size_mib}mib.bin"
    return args


def main():
    args = parse_args()
    ensure_input_file(args.file, args.size_mib)

    file_upload.call_with_oci_token_refresh = _stub_token_refresh
    server, state, endpoint = start_registry(args.throttle_mibs)
    mode = (f"throttled to {args.throttle_mibs:g} MiB/s aggregate"
            if args.throttle_mibs else "loopback-max")
    print(f"Registry: {endpoint} ({mode})")
    print(f"Timing {args.runs} run(s) of upload_file (chunked-v2 forced) ...")

    try:
        results = [
            run_once(i, endpoint, state, args.file, args.size_mib)
            for i in range(1, args.runs + 1)
        ]
    finally:
        server.shutdown()

    print()
    print(f"Machine: {platform.platform()}")
    print(f"CPU:     {cpu_brand()} ({os.cpu_count()} logical cores)")
    print(f"Input:   {args.size_mib} MiB deterministic (seed {GEN_SEED:#x}); {mode}")
    print(f"Median:  {statistics.median(d for d, _ in results):.2f}s  "
          f"({statistics.median(m for _, m in results):.1f} MiB/s)")


if __name__ == "__main__":
    main()
