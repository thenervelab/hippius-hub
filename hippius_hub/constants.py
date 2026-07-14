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

# ----- chunked-artifact annotation keys (shared by the pack layout below) -----
# Whole-file metadata carried once, on the pointer layer's annotations, so the
# metadata read path (siblings/list_repo_files) never fetches the pointer blob.
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

# Hard ceiling on a pack. The downloader buffers a pack whole in memory to verify and
# carve it, so a pack's declared size is attacker-controlled input on the way back in
# (a malicious registry can claim any layer size); `MAX_PACK_SIZE` in
# src/chunk_fetcher.rs rejects a declaration above this. Enforcing the SAME bound here
# means an over-large HIPPIUS_PACK_SIZE fails at upload — where the operator can act on
# it — instead of silently producing packs that every reader then refuses to fetch.
# The two constants must stay equal; `pack_size_cap_matches_python` pins them.
MAX_PACK_SIZE = 512 * 1024 * 1024

# CNCF Distribution hard-caps a manifest PUT body at 4 MiB (maxManifestBodySize).
# Past it the registry returns an opaque 400 — AFTER every blob is already
# uploaded. We check the serialized manifest before the PUT and raise a clear
# error instead (errors.ManifestTooLargeError). Only reachable by an artifact
# with tens of thousands of chunk layers (a >1 TB single file at the 64 MiB chunk
# average); the real fix for such artifacts is Referrers/index fan-out (a
# documented follow-up in the chunked-artifact plan).
MAX_MANIFEST_BYTES = 4 * 1024 * 1024

# Layout values THIS build can read. The forward-compat guard (_oci._guard_layout)
# refuses any other value loudly rather than misparsing. chunked WRITES stay opt-in
# for this release (resolve_chunked_write_enabled defaults off): readers upgrade
# first, writers flip the default on in a later release.
KNOWN_LAYOUTS: frozenset = frozenset({CHUNKED_LAYOUT_V2})


# ----- transfer tuning knobs -----
# Defaults mirror the Rust-side constants in src/chunked_downloader.rs and
# src/chunk_fetcher.rs so the native engine and the Python layer agree. Each is overridable via env so a
# user on a slow/restricted link (or we, while diagnosing) can A/B them without
# rebuilding the extension.
DEFAULT_CHUNK_SIZE = 100 * 1024 * 1024  # 100 MB; mirrors DEFAULT_CHUNK_SIZE
DEFAULT_MAX_CONCURRENT = 32             # default parallel connections; mirrors chunk_fetcher::DOWNLOAD_POOL_MAX_IDLE
DEFAULT_CONNECT_TIMEOUT = 30            # seconds; mirrors connect_timeout
DEFAULT_TRANSFER_WORKERS = 8            # snapshot/upload ThreadPoolExecutor size


# Chunked-artifact upload. A file at or above the threshold is stored as a
# pointer + content-defined chunks packed into ~64 MiB pack blobs (chunked-v2);
# below it, one plain blob (byte-identical to the pre-chunking layout). The CDC
# average is the FastCDC target (min/max derived avg/4..avg*4 in the Rust
# splitter) and is part of the wire contract — a change means a new layout
# version, not a silent retune.
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
    """Target pack size (bytes) for the chunked-v2 layout. Overridable for testing.

    Bounded above by `MAX_PACK_SIZE`: the reader buffers a pack whole to verify and
    carve it, and refuses a pack declaring more than that, so a larger value here
    would write an artifact nothing can read back. Fail at upload, loudly, rather
    than at every future download.
    """
    value = _resolve_positive_int("HIPPIUS_PACK_SIZE", DEFAULT_PACK_SIZE)
    if value > MAX_PACK_SIZE:
        raise ValueError(
            f"HIPPIUS_PACK_SIZE must be at most {MAX_PACK_SIZE} bytes "
            f"(the reader's per-pack maximum), got {value}"
        )
    return value


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
    """Per-read stall timeout (seconds) for real transfers, or None to leave it off.

    Threaded into download_file_native / download_packs_native, where the shared
    reqwest 0.12 client applies it as ``.read_timeout()`` when set — it fires only
    when a read STALLS (no byte within the window, reset on each successful read),
    bounding a dribbling/stalled chunk without capping an honest slow transfer.
    This now reaches real transfers, not only ``hippius-hub diagnose`` (audit L9).

    Opt-in (``None`` by default = no client read timeout): a client-level read
    timeout is a global setting on the one process-wide download client shared by
    every concurrent chunk, so it stays off by default (the default client is
    byte-identical to the pre-audit one) and a user opts in per transfer. The
    default-on download stall guard (M4) is deferred to a per-chunk app-level
    idle-timeout. (The prior "reqwest 0.11 only offers a total timeout" note was
    stale — the crate has been on reqwest 0.12, which diagnostics already used.)"""
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
