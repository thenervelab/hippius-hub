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

# ----- chunked-v2 pack layout (docs/plans/2026-07-10-chunked-v2-pack-layout.md) -----
# Same 4 MiB CDC dedup chunks as v1, but transferred in ~64 MiB *pack* blobs: the
# pointer BLOB (fetched on download, not just annotations) maps each chunk to
# (pack digest, offset, size). This cuts per-file upload round-trips ~15x. The pack
# blob is opaque bytes (its format never changes); only the *pointer* schema
# versions, so pack layers stay `pack.v1` even as the pointer goes v2+. A file's
# manifest lists its pointer layer plus the union of pack blobs it references (new
# packs AND packs reused-by-range from prior revisions) so every referenced pack
# stays GC-safe and pullable.
CHUNKED_LAYOUT_V2 = "chunked-v2"
ARTIFACT_TYPE_CHUNKED_V2 = "application/vnd.hippius.chunked.v2"
POINTER_MEDIA_TYPE_V2 = "application/vnd.hippius.pointer.v2"
PACK_MEDIA_TYPE = "application/vnd.hippius.pack.v1"
# Target pack size. New chunks are concatenated in file order into a pack, closing
# it once it reaches this; a pack may overshoot by at most one chunk (<= fastcdc
# MAXIMUM_MAX = 16 MiB) and has no minimum (a 1-chunk edit yields one small pack).
# 64 MiB = HF-Xet's block/xorb size (~16 chunks/pack) — the transfer unit we could
# not use in v1 because fastcdc caps a single CHUNK at 4 MiB; packing lifts that.
DEFAULT_PACK_SIZE = 64 * 1024 * 1024

# CNCF Distribution hard-caps a manifest PUT body at 4 MiB (maxManifestBodySize).
# Past it the registry returns an opaque 400 — AFTER every blob is already
# uploaded. We check the serialized manifest before the PUT and raise a clear
# error instead (errors.ManifestTooLargeError). Only reachable by an artifact
# with tens of thousands of chunk layers (a >1 TB single file at the 64 MiB chunk
# average); the real fix for such artifacts is Referrers/index fan-out (a
# documented follow-up in the chunked-artifact plan).
MAX_MANIFEST_BYTES = 4 * 1024 * 1024

# Layout values THIS build can read. The forward-compat guard (_oci._guard_layout)
# and chunked-read support ship together in this release — there is no earlier
# build carrying an empty-floor guard, so no already-deployed reader refuses a
# chunked artifact. That backward gap is why chunked WRITES are opt-in for this
# release (resolve_chunked_write_enabled defaults off): readers upgrade first,
# writers flip the default on in a later release.
KNOWN_LAYOUTS: frozenset = frozenset({CHUNKED_LAYOUT, CHUNKED_LAYOUT_V2})


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
# 4 MiB is the LARGEST average fastcdc 3.2.1 can produce: its AVERAGE_MAX cap is
# 4 MiB, and via min=avg/4 / max=avg*4 the derived 1 MiB min and 16 MiB max are
# that crate's MINIMUM_MAX / MAXIMUM_MAX ceilings exactly. A larger average — the
# original 64 MiB "HF value" — panics the splitter (min=16 MiB > MINIMUM_MAX).
# (HF's 64 MiB is a transfer block aggregating many small CDC chunks, not a CDC
# average.) Smaller = finer dedup but more blobs; 4 MiB keeps blob counts low
# while staying inside the caps. src/uploader.rs enforces the same bound.
DEFAULT_CDC_AVG_SIZE = 4 * 1024 * 1024        # 4 MiB (fastcdc AVERAGE_MAX ceiling)


def resolve_chunk_threshold() -> int:
    """Minimum file size (bytes) stored via the chunked layout."""
    return _resolve_positive_int("HIPPIUS_CHUNK_THRESHOLD", DEFAULT_CHUNK_THRESHOLD)


def resolve_cdc_avg_size() -> int:
    """FastCDC average chunk size (bytes). Pinned; overridable only for testing."""
    return _resolve_positive_int("HIPPIUS_CDC_AVG_SIZE", DEFAULT_CDC_AVG_SIZE)


def resolve_pack_size() -> int:
    """Target pack size (bytes) for the chunked-v2 layout. Overridable for testing."""
    return _resolve_positive_int("HIPPIUS_PACK_SIZE", DEFAULT_PACK_SIZE)


def resolve_chunked_layout() -> str:
    """Which chunked layout new large-file uploads emit: 'chunked-v1' (default) or
    'chunked-v2' (Xet-style packs). Opt-in for v2 the same way writes are opt-in:
    v2 is a NEW layout value, so a reader that predates v2 refuses it loudly rather
    than misreading — flip the default to v2 only once the v2-capable reader is the
    deployed floor. `HIPPIUS_CHUNKED_LAYOUT=v2` opts a producer in now (staging)."""
    raw = (os.environ.get("HIPPIUS_CHUNKED_LAYOUT") or "").strip().lower()
    if raw in ("v2", "chunked-v2", "2"):
        return CHUNKED_LAYOUT_V2
    if raw in ("", "v1", "chunked-v1", "1"):
        return CHUNKED_LAYOUT
    raise ValueError(
        f"HIPPIUS_CHUNKED_LAYOUT must be 'v1' or 'v2', got {raw!r}"
    )


def resolve_chunked_write_enabled() -> bool:
    """Whether large files upload chunked, or as one plain blob (default).

    The rollout gate — opt-in for this release. The forward-compat guard that
    makes an un-upgraded reader refuse a chunked artifact loudly
    (UnsupportedLayoutError) ships in this SAME release, so no already-deployed
    reader (<= v0.5.1) carries it. Such a reader instead matches the pointer
    layer by its title and silently writes the ~200-byte pointer blob AS the
    file — undetectable even with HIPPIUS_VERIFY_HASH=1, since it checks the
    pointer blob against its own digest. Defaulting off keeps large-file uploads
    byte-identical to the pre-chunking layout until the guard-bearing reader is
    universally deployed; a later release flips the default on. Set
    HIPPIUS_CHUNKED_WRITE=1 to opt a producer in now (e.g. on staging)."""
    raw = os.environ.get("HIPPIUS_CHUNKED_WRITE")
    if not raw or not raw.strip():
        return False
    return raw.strip().lower() in ("1", "true", "yes", "on")


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


def resolve_max_inflight_packs() -> int:
    """Cap on chunked-v2 packs being uploaded across ALL files at once.

    Folder uploads nest parallelism — `upload_folder` runs files concurrently and
    each file uploads its packs concurrently — so without a shared bound peak
    resident memory is `file_workers × pack_workers × pack_size`, ~4 GB at the
    defaults (and higher once per-pack I/O and HTTP buffers count). This bounds the
    product to one ceiling. Defaults to `resolve_upload_workers()` so a single-file
    upload keeps its current concurrency; only the multiplying folder case is
    reined in. Raising it buys nothing when the link is bandwidth-bound (the
    Harbor-flow probe measures ~0.9× throughput scaling from 1→16 connections)."""
    return _resolve_positive_int("HIPPIUS_MAX_INFLIGHT_PACKS", resolve_upload_workers())


def debug_enabled() -> bool:
    """True when verbose transport logging is requested (HIPPIUS_DEBUG truthy
    or RUST_LOG set). The CLI also flips HIPPIUS_DEBUG on for `--verbose`."""
    if os.environ.get("RUST_LOG"):
        return True
    return os.environ.get("HIPPIUS_DEBUG", "").lower() in ("1", "true", "yes")
