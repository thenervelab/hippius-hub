"""Shared defaults: cache dirs, registry/API URLs, HTTP timeouts, OCI media types.

Single source of truth so the env-var override knobs (HIPPIUS_API_URL etc.)
don't need to be re-implemented in each module.
"""
import os
from typing import Optional
from urllib.parse import urlparse

# http:// is only tolerated for the receiver when it points at the local host
# (port-forward / local dev). Any other host over http would put a repo-scoped
# Harbor push token on the wire in cleartext — see resolve_receiver_url.
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", ""})

DEFAULT_CACHE_DIR = os.path.expanduser("~/.cache/hippius/hub")
DEFAULT_REGISTRY_URL = "https://registry.hippius.com"

# Hippius backend (account, plans, registry namespaces, model index). The
# CLI talks to this for everything except raw blob/manifest IO, which goes
# straight to the OCI registry.
DEFAULT_API_URL = os.environ.get("HIPPIUS_API_URL", "https://api.hippius.com")
API_TOKEN_PATH = os.path.join(DEFAULT_CACHE_DIR, "api_token")


def resolve_registry(endpoint: Optional[str]) -> str:
    """Return the user-provided endpoint (trailing slash trimmed) or the default."""
    return (endpoint or DEFAULT_REGISTRY_URL).rstrip("/")

# httpx timeout for control-plane calls (manifest fetch, blob HEAD, registry
# admin API). Long enough to absorb edge-proxy stalls; not used by the Rust
# blob fetcher.
DEFAULT_HTTP_TIMEOUT = 30.0

# OCI media-type Accept header for manifest reads (v1 and Docker fallback).
OCI_MANIFEST_ACCEPT = (
    "application/vnd.oci.image.manifest.v1+json, "
    "application/vnd.docker.distribution.manifest.v2+json"
)

# OCI manifest annotation that carries the in-repo filename for a layer.
LAYER_TITLE_KEY = "org.opencontainers.image.title"


# ----- transfer tuning knobs -----
# Defaults mirror the Rust-side constants in src/chunked_downloader.rs so the
# native engine and the Python layer agree. Each is overridable via env so a
# user on a slow/restricted link (or we, while diagnosing) can A/B them without
# rebuilding the extension.
DEFAULT_CHUNK_SIZE = 100 * 1024 * 1024  # 100 MB; mirrors DEFAULT_CHUNK_SIZE
DEFAULT_MAX_CONCURRENT = 32             # mirrors MAX_CONCURRENT_DOWNLOADS
DEFAULT_CONNECT_TIMEOUT = 30            # seconds; mirrors connect_timeout
DEFAULT_TRANSFER_WORKERS = 8            # snapshot/upload ThreadPoolExecutor size

# Multipart upload routing. A blob is eligible for the parallel receiver path
# only when it is at least this large AND a receiver URL is configured — below
# the threshold, the per-blob parallelism overhead (part planning, an extra
# in-cluster hop) costs more than the single-stream PUT saves. 256 MB is a
# provisional floor; the real knee (where N-way beats single-stream into
# harbor-registry) comes from the in-cluster diagnose measurement and should
# replace this default once measured.
DEFAULT_MULTIPART_THRESHOLD = 256 * 1024 * 1024  # 256 MB

# Requested bytes per part. The receiver may clamp this and returns the value
# it actually used; the client derives its part count from that. Parts hit the
# receiver (NOT S3), so there is no 5 MB S3-multipart floor — 64 MB keeps the
# part count small (a 2.6 GB shard is ~41 parts) so the part-plan and any
# re-drive stay cheap.
DEFAULT_MULTIPART_PART_SIZE = 64 * 1024 * 1024  # 64 MB


def _resolve_positive_int(env_var: str, default: int) -> int:
    """Read a positive-int env var, falling back to `default` when unset.
    Raises ValueError on a non-positive value so misconfiguration surfaces
    immediately rather than silently producing a degenerate transfer."""
    raw = os.environ.get(env_var)
    if not raw:
        return default
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{env_var} must be a positive integer, got {value}")
    return value


def resolve_chunk_size() -> int:
    return _resolve_positive_int("HIPPIUS_CHUNK_SIZE", DEFAULT_CHUNK_SIZE)


def resolve_max_concurrent() -> int:
    return _resolve_positive_int("HIPPIUS_MAX_CONCURRENT", DEFAULT_MAX_CONCURRENT)


def resolve_connect_timeout() -> int:
    return _resolve_positive_int("HIPPIUS_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT)


def resolve_read_timeout() -> Optional[int]:
    """Per-chunk total request timeout (seconds), or None to leave it unset.
    Unset by default: reqwest 0.11 only offers a *total* request timeout, so a
    value here bounds a single chunk request (each chunk is its own request),
    not the whole file. Opting in protects against a silently-stalled chunk."""
    raw = os.environ.get("HIPPIUS_READ_TIMEOUT")
    if not raw:
        return None
    value = int(raw)
    if value <= 0:
        raise ValueError(f"HIPPIUS_READ_TIMEOUT must be a positive integer, got {value}")
    return value


def resolve_verify_hash() -> bool:
    return os.environ.get("HIPPIUS_VERIFY_HASH", "").lower() in ("1", "true", "yes")


def resolve_snapshot_workers() -> int:
    return _resolve_positive_int("HIPPIUS_SNAPSHOT_WORKERS", DEFAULT_TRANSFER_WORKERS)


def resolve_upload_workers() -> int:
    return _resolve_positive_int("HIPPIUS_UPLOAD_WORKERS", DEFAULT_TRANSFER_WORKERS)


def resolve_multipart_threshold() -> int:
    """Minimum blob size (bytes) eligible for the parallel receiver path."""
    return _resolve_positive_int("HIPPIUS_MULTIPART_THRESHOLD", DEFAULT_MULTIPART_THRESHOLD)


def resolve_multipart_part_size() -> int:
    """Requested bytes per part for the receiver path (receiver may clamp)."""
    return _resolve_positive_int("HIPPIUS_MULTIPART_PART_SIZE", DEFAULT_MULTIPART_PART_SIZE)


def resolve_receiver_url() -> Optional[str]:
    """Base URL of the in-cluster blob receiver, or None when unset.

    This is the feature gate: when `HIPPIUS_RECEIVER_URL` is unset the multipart
    path is entirely off and uploads behave byte-for-byte as they do today (a
    single streaming PUT straight to the registry). Trailing slash trimmed so
    callers can join paths without doubling it — mirrors `resolve_registry`.
    An empty/whitespace-only value is treated as unset rather than as a URL of
    "", so `HIPPIUS_RECEIVER_URL=` in a shell profile disables cleanly instead
    of producing malformed request URLs.

    The scheme is enforced: the client forwards its repo-scoped Harbor push
    token to the receiver (which replays it to Harbor), so an `http://` hop to a
    non-loopback host would leak that credential in cleartext. Such a value is
    rejected loudly rather than silently downgraded; `http://localhost` stays
    allowed for local port-forward testing.
    """
    raw = os.environ.get("HIPPIUS_RECEIVER_URL")
    if raw is None or not raw.strip():
        return None
    url = raw.strip().rstrip("/")
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(
            f"HIPPIUS_RECEIVER_URL must be an http(s) URL, got scheme {parsed.scheme!r}"
        )
    if parsed.scheme == "http" and (parsed.hostname or "") not in _LOOPBACK_HOSTS:
        raise ValueError(
            "HIPPIUS_RECEIVER_URL uses http:// to a non-loopback host "
            f"({parsed.hostname!r}); the client sends a repo-scoped Harbor push "
            "token to the receiver, so a cleartext hop would leak it. Use https:// "
            "(http:// is allowed only for localhost port-forward testing)."
        )
    return url


def debug_enabled() -> bool:
    """True when verbose transport logging is requested (HIPPIUS_DEBUG truthy
    or RUST_LOG set). The CLI also flips HIPPIUS_DEBUG on for `--verbose`."""
    if os.environ.get("RUST_LOG"):
        return True
    return os.environ.get("HIPPIUS_DEBUG", "").lower() in ("1", "true", "yes")
