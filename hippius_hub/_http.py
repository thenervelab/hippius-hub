"""Process-wide pooled httpx.Client for the control plane.

Every registry/console control-plane request (OCI manifest GET/PUT, blob HEAD,
token fetch, Harbor admin API, console API, the PyPI update check) used to go
through httpx's module-level helpers (``httpx.get``/``post``/...). Each of those
builds and tears down a throwaway ``Client`` -- and therefore opens a fresh
TCP+TLS handshake -- to the *same* registry host every call. One new-file upload
chains on the order of a dozen such handshakes; a folder of N files ~2N+3. The
Rust data plane already holds its ``reqwest`` client in a process-global
``OnceLock`` precisely to keep connections warm across blobs; this module is the
Python-side equivalent for the metadata I/O the control plane does.

``httpx.Client`` is the documented connection-pooling primitive and is safe to
share across threads for concurrent synchronous requests (httpcore's sync pool
serializes its own bookkeeping), so the folder/snapshot upload/download
ThreadPoolExecutors and the metadata fan-outs all share this single instance.

Contract: this module owns ONLY the transport pool. Every call site still passes
its own ``headers=``/``timeout=``/``params=`` exactly as before. Two deliberate,
benign differences from the old throwaway-per-call clients: a process-wide cookie
jar now persists ``Set-Cookie`` across same-origin control-plane calls (httpx
scopes cookies by domain, so nothing crosses origins -- a sticky-session LB
benefits), and idle connections are reused between calls. No auth is ever baked
into the client -- credentials are per-request headers -- so it can never forward
one origin's credential to another.
"""
from __future__ import annotations

import atexit
import threading
from typing import Optional

import httpx

_CLIENT: Optional[httpx.Client] = None
_CLIENT_LOCK = threading.Lock()


def client() -> httpx.Client:
    """Return the process-wide shared ``httpx.Client``, built on first use.

    Lazy on purpose: importing ``hippius_hub`` never opens a pool, and the client
    is created inside whatever ``respx`` mock context the caller runs under (respx
    patches the httpx transport at the class level, so a client built at any time
    is intercepted like any other). Double-checked locking keeps a single instance
    under concurrent first callers without paying a lock on the steady-state path.
    """
    global _CLIENT
    if _CLIENT is None:
        with _CLIENT_LOCK:
            if _CLIENT is None:
                _CLIENT = httpx.Client(
                    # Keep connections warm across one operation's control-plane
                    # sequence (token -> manifest -> config/blob HEADs -> PUT...);
                    # httpx's 5s default idle expiry would drop them between steps.
                    # max_connections=None keeps the pre-pooling property that
                    # concurrent control-plane requests are never capped process-wide
                    # (each throwaway client opened its own connection), so a high
                    # upload_folder(max_workers=) / HIPPIUS_SNAPSHOT_WORKERS never
                    # trips a shared httpx.PoolTimeout that the old path couldn't
                    # produce; up to 32 idle connections are still retained for reuse.
                    limits=httpx.Limits(
                        max_keepalive_connections=32,
                        max_connections=None,
                        keepalive_expiry=30.0,
                    ),
                    # No client-level timeout: the call sites that pass timeout= keep
                    # theirs; the few Harbor admin calls that pass none inherit the
                    # Client default, which is httpx's Timeout(5.0) -- identical to
                    # the pre-pooling module-level httpx.get default.
                )
                atexit.register(_close)
    return _CLIENT


def _close() -> None:
    """Close the shared client's connection pool at interpreter exit."""
    global _CLIENT
    existing, _CLIENT = _CLIENT, None
    if existing is not None:
        existing.close()
