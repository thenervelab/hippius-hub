"""Shared defaults: cache dirs, registry/API URLs, HTTP timeouts, OCI media types.

Single source of truth so the env-var override knobs (HIPPIUS_API_URL etc.)
don't need to be re-implemented in each module.
"""
import os
from typing import Optional

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

# Manifest-level annotation marking a hippius-specific artifact layout. A client
# that does not recognize the value MUST refuse the artifact rather than misread
# it: a chunked file resolved by a layout-blind client would write the tiny
# pointer blob verbatim as the file. See `_oci._guard_layout` /
# `errors.UnsupportedLayoutError`. Absent annotation = the pre-chunking layout
# (one plain titled blob per file), which every build reads.
LAYOUT_ANNOTATION_KEY = "com.hippius.layout"

# ----- chunked-artifact layout (docs/plans/2026-07-09-chunked-artifact-layout.md) -----
# A file >= the chunk threshold is stored as one titled *pointer* layer plus K
# *untitled* content-defined chunk blobs. These media types + annotation keys are
# the wire contract shared by the uploader (writes them), the reader (parses
# them), and the layout guard (gates them). Bump CHUNKED_LAYOUT to a new value if
# any of this shape changes — a mismatched reader must fail the guard, not
# misparse (see errors.UnsupportedLayoutError).
CHUNKED_LAYOUT = "chunked-v1"
ARTIFACT_TYPE_CHUNKED = "application/vnd.hippius.chunked.v1"
POINTER_MEDIA_TYPE = "application/vnd.hippius.pointer.v1"
CHUNK_MEDIA_TYPE = "application/vnd.hippius.chunk.v1"
# Whole-file metadata carried once, on the pointer layer's annotations (the K
# chunk layers stay bare — mediaType+digest+size only — to keep the manifest small).
FILE_SIZE_KEY = "com.hippius.file.size"
FILE_DIGEST_KEY = "com.hippius.file.digest"
CHUNK_COUNT_KEY = "com.hippius.chunk.count"

# Layout values THIS build can read. The compatibility guard shipped first as an
# empty floor; chunked-read support (Phase 1) adds its value here in the same
# commit that teaches the client to parse it.
KNOWN_LAYOUTS: frozenset = frozenset({CHUNKED_LAYOUT})


# ----- transfer tuning knobs -----
# Defaults mirror the Rust-side constants in src/chunked_downloader.rs so the
# native engine and the Python layer agree. Each is overridable via env so a
# user on a slow/restricted link (or we, while diagnosing) can A/B them without
# rebuilding the extension.
DEFAULT_CHUNK_SIZE = 100 * 1024 * 1024  # 100 MB; mirrors DEFAULT_CHUNK_SIZE
DEFAULT_MAX_CONCURRENT = 32             # mirrors MAX_CONCURRENT_DOWNLOADS
DEFAULT_CONNECT_TIMEOUT = 30            # seconds; mirrors connect_timeout
DEFAULT_TRANSFER_WORKERS = 8            # snapshot/upload ThreadPoolExecutor size


# Chunked-artifact upload. A file at or above the threshold is stored as a
# pointer + K content-defined chunk blobs; below it, one plain blob (byte-
# identical to the pre-chunking layout). The CDC average is the FastCDC target
# (min/max derived avg/4..avg*4 in the Rust splitter) and is part of the wire
# contract — a change means a new layout version, not a silent retune.
DEFAULT_CHUNK_THRESHOLD = 256 * 1024 * 1024  # 256 MiB
DEFAULT_CDC_AVG_SIZE = 64 * 1024 * 1024       # 64 MiB (HF-Xet block/transfer size)


def resolve_chunk_threshold() -> int:
    """Minimum file size (bytes) stored via the chunked layout."""
    return _resolve_positive_int("HIPPIUS_CHUNK_THRESHOLD", DEFAULT_CHUNK_THRESHOLD)


def resolve_cdc_avg_size() -> int:
    """FastCDC average chunk size (bytes). Pinned; overridable only for testing."""
    return _resolve_positive_int("HIPPIUS_CDC_AVG_SIZE", DEFAULT_CDC_AVG_SIZE)


def resolve_chunked_write_enabled() -> bool:
    """Whether large files upload chunked (default) or as one plain blob.

    The rollout gate. Chunked writes are safe to publish because a reader that
    predates chunked support fails LOUDLY (UnsupportedLayoutError + upgrade hint)
    rather than corrupting — so this defaults on. An operator mid-rollout can set
    HIPPIUS_CHUNKED_WRITE=0 to keep emitting the pre-chunking single-blob layout
    until every consumer has the chunk-aware reader deployed."""
    raw = os.environ.get("HIPPIUS_CHUNKED_WRITE")
    if not raw or not raw.strip():
        return True
    return raw.strip().lower() not in ("0", "false", "no", "off")


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


def debug_enabled() -> bool:
    """True when verbose transport logging is requested (HIPPIUS_DEBUG truthy
    or RUST_LOG set). The CLI also flips HIPPIUS_DEBUG on for `--verbose`."""
    if os.environ.get("RUST_LOG"):
        return True
    return os.environ.get("HIPPIUS_DEBUG", "").lower() in ("1", "true", "yes")
