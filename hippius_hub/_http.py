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

Contract: this module owns ONLY the transport pool and stays as stateless per
request as the old throwaway-per-call clients. Every call site still passes its own
``headers=``/``timeout=``/``params=`` exactly as before, and the one behavioral
difference a shared client would introduce -- a persistent cookie jar -- is
deliberately neutralized by a reject-all cookie policy (see ``_NoCookies``): no
``Set-Cookie`` is ever stored or replayed. That is load-bearing against Harbor,
which exempts token-authenticated requests from CSRF only while no session cookie
is present; a *persisted* Harbor session cookie flips it into browser-session mode
and it then rejects a state-changing request (manifest PUT) with 403 "CSRF token
not found in request." The old per-call clients never persisted cookies, so they
stayed CSRF-exempt; this matches that exactly. No auth is baked into the client
either -- credentials are per-request headers -- so it can never forward one
origin's credential to another.
"""
from __future__ import annotations

import atexit
import http.cookiejar
import threading
from typing import Optional

import httpx


class _NoCookies(http.cookiejar.DefaultCookiePolicy):
    """Cookie policy that rejects every ``Set-Cookie`` (see the module docstring).

    Returning ``False`` from ``set_ok`` for every cookie means the shared client
    stores nothing and therefore sends nothing -- each request is as stateless as a
    fresh throwaway client, so a Harbor session/CSRF cookie is never replayed onto a
    later state-changing request.
    """

    def set_ok(self, cookie, request):
        return False


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
                # Reject all cookies so a Harbor session/CSRF cookie can never be
                # persisted and replayed on a later state-changing request (which
                # would 403 with "CSRF token not found") -- keep the shared client as
                # stateless as the old per-call clients. See _NoCookies.
                _CLIENT.cookies.jar.set_policy(_NoCookies())
                atexit.register(_close)
    return _CLIENT


def _close() -> None:
    """Close the shared client's connection pool at interpreter exit."""
    global _CLIENT
    existing, _CLIENT = _CLIENT, None
    if existing is not None:
        existing.close()
