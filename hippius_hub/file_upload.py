"""Upload path: `upload_file` / `upload_folder` against the OCI registry.

Hashes each file, pushes its blob via the Rust extension, then merges the
layer list into the existing manifest and PUTs it with `If-Match` so a
concurrent writer's manifest can't be silently clobbered. Folder uploads
parallelise per-file via a ThreadPoolExecutor.
"""
import datetime
import hashlib
import json
import os
import random
import tempfile
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import BinaryIO, Dict, List, Optional, Union

import httpx
from huggingface_hub import CommitInfo
from huggingface_hub.utils import filter_repo_objects
from tqdm import tqdm

from . import _http
from ._oci import fetch_manifest, group_files, layer_title, parse_pointer_v2
from ._packing import plan_packs, pointer_v2_bytes, resolve_pointer_chunks
from .auth import get_oci_bearer_token
from .constants import (
    ARTIFACT_TYPE_CHUNKED_V2,
    CHUNK_COUNT_KEY,
    CHUNKED_LAYOUT_V2,
    DEFAULT_HTTP_TIMEOUT,
    FILE_DIGEST_KEY,
    FILE_SIZE_KEY,
    LAYER_TITLE_KEY,
    LAYOUT_ANNOTATION_KEY,
    MAX_MANIFEST_BYTES,
    PACK_MEDIA_TYPE,
    POINTER_MEDIA_TYPE_V2,
    resolve_cdc_avg_size,
    resolve_chunk_threshold,
    resolve_chunked_write_enabled,
    resolve_max_inflight_packs,
    resolve_pack_size,
    resolve_registry,
    resolve_upload_workers,
)
from .errors import ConcurrentManifestUpdateError, MalformedManifestError, ManifestTooLargeError
from .file_download import _oci_repo_path, _validate_repo_type

try:
    from .hippius_core import (
        chunk_and_hash_native,
        hash_file_native,
        pack_upload_native,
        upload_blob_native,
    )
except ImportError:
    raise ImportError("hippius_core is not installed. Did you run `maturin develop`?")


# ---- helpers ----

def _oci_bearer(repo_id: str, token, push: bool = True, endpoint=None) -> str:
    # Token resolution + the off-origin credential guard happen inside
    # get_oci_bearer_token, which mints from `resolve_registry(endpoint)`.
    return get_oci_bearer_token(repo_id, token, push=push, endpoint=endpoint)


def _empty_config_blob_descriptor() -> tuple:
    data = b"{}"
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
    return data, digest, len(data)


def _upload_blob_single_put(registry: str, repo_id: str, oci_token: str, file_path: str, digest: str) -> None:
    """OCI blob-upload init against the registry, then one streaming PUT-with-digest
    straight to it. This is the path for a plain (sub-threshold) whole-file blob;
    large files go through the chunked-v2 path instead (`_upload_file_chunked_v2`)."""
    headers = {"Authorization": f"Bearer {oci_token}"}
    init_headers = {**headers, "Content-Length": "0"}
    init = _http.client().post(f"{registry}/v2/{repo_id}/blobs/uploads/", headers=init_headers, timeout=DEFAULT_HTTP_TIMEOUT)
    init.raise_for_status()
    location = init.headers.get("Location")
    if not location:
        raise ValueError("Registry did not return a Location header for upload initiation")
    if location.startswith("/"):
        location = f"{registry}{location}"
    sep = "&" if "?" in location else "?"
    upload_blob_native(f"{location}{sep}digest={digest}", file_path, oci_token)


def _ensure_blob_uploaded(
    registry: str,
    repo_id: str,
    oci_token: str,
    file_path: str,
    sha256_hash: str,
) -> bool:
    """Upload a plain whole-file blob if not already present at its digest.
    Returns True if a new upload happened, False if the blob already existed and
    was skipped (the dedup HEAD)."""
    digest = f"sha256:{sha256_hash}"
    headers = {"Authorization": f"Bearer {oci_token}"}
    check = _http.client().head(f"{registry}/v2/{repo_id}/blobs/{digest}", headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
    if check.status_code == 200:
        return False
    _upload_blob_single_put(registry, repo_id, oci_token, file_path, digest)
    return True


# Process-wide record of (registry, oci_repo) whose empty-config blob we've
# confirmed present (a 200 HEAD, or our own successful PUT). The empty `{}` config
# is the SAME 2-byte blob for every manifest and stays referenced -- so never
# GC-eligible -- while any tag exists, so re-HEADing it before every upload to a
# repo we've already confirmed is a pure wasted round-trip. Keyed per (registry,
# repo): a blob in repo A implies nothing about repo B, and the registry is part of
# the key so a custom endpoint is never assumed from the default.
_config_blob_present: set = set()
_config_blob_lock = threading.Lock()


def clear_config_blob_cache() -> None:
    """Drop the confirmed-config-blob cache. For tests reusing a (registry, repo)
    key across cases -- the process-wide cache would otherwise skip a HEAD the test
    set a respx route up for."""
    with _config_blob_lock:
        _config_blob_present.clear()


def _ensure_config_blob_uploaded(registry: str, repo_id: str, oci_token: str) -> tuple:
    """Push the empty-object config blob if missing. Returns (digest, size).

    Skips the HEAD once this (registry, repo) is confirmed (see
    `_config_blob_present`). Edge: if every tag in the repo were deleted mid-process
    and Harbor GC then reaped the `{}` config, the skip means the following manifest
    PUT sees a 400 MANIFEST_BLOB_UNKNOWN. `_put_manifest` evicts this cache entry on
    a persistent BLOB_UNKNOWN, so re-running the upload re-HEADs/re-PUTs the config
    rather than skipping and failing the same way again."""
    data, digest, size = _empty_config_blob_descriptor()
    cache_key = (registry, repo_id)
    with _config_blob_lock:
        if cache_key in _config_blob_present:
            return digest, size
    headers = {"Authorization": f"Bearer {oci_token}"}
    check = _http.client().head(f"{registry}/v2/{repo_id}/blobs/{digest}", headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
    if check.status_code != 200:
        init = _http.client().post(
            f"{registry}/v2/{repo_id}/blobs/uploads/",
            headers={**headers, "Content-Length": "0"},
            timeout=DEFAULT_HTTP_TIMEOUT,
        )
        init.raise_for_status()
        loc = init.headers.get("Location")
        # Guard a missing Location (audit L-LOCATION-GUARD): a 2xx init without the
        # header would make `"?" in loc` raise an opaque TypeError on None. Match the
        # clear error the sibling helpers (`_ensure_bytes_blob_uploaded`) raise.
        if not loc:
            raise ValueError("Registry did not return a Location header for upload initiation")
        if loc.startswith("/"):
            loc = f"{registry}{loc}"
        sep = "&" if "?" in loc else "?"
        # Raise on a failed config PUT: otherwise the manifest PUT that follows
        # fails later with an opaque MANIFEST_BLOB_UNKNOWN instead of the real
        # cause (matches _ensure_bytes_blob_uploaded / _upload_blob_single_put).
        put = _http.client().put(
            f"{loc}{sep}digest={digest}",
            headers={**headers, "Content-Type": "application/octet-stream"},
            content=data,
            timeout=DEFAULT_HTTP_TIMEOUT,
        )
        put.raise_for_status()
    # Confirmed present now (the HEAD hit, or we just PUT it) -- skip the HEAD on
    # later uploads to this repo in this process.
    with _config_blob_lock:
        _config_blob_present.add(cache_key)
    return digest, size


def _ensure_bytes_blob_uploaded(registry: str, repo_id: str, oci_token: str, data: bytes, digest: str) -> None:
    """Push an in-memory blob (the pointer blob) if not already present at its
    digest. HEAD-dedups first, then runs the OCI init + PUT-with-digest dance."""
    headers = {"Authorization": f"Bearer {oci_token}"}
    check = _http.client().head(f"{registry}/v2/{repo_id}/blobs/{digest}", headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
    if check.status_code == 200:
        return
    init = _http.client().post(f"{registry}/v2/{repo_id}/blobs/uploads/", headers={**headers, "Content-Length": "0"}, timeout=DEFAULT_HTTP_TIMEOUT)
    init.raise_for_status()
    location = init.headers.get("Location")
    if not location:
        raise ValueError("Registry did not return a Location header for upload initiation")
    if location.startswith("/"):
        location = f"{registry}{location}"
    sep = "&" if "?" in location else "?"
    put = _http.client().put(f"{location}{sep}digest={digest}", headers={**headers, "Content-Type": "application/octet-stream"}, content=data, timeout=DEFAULT_HTTP_TIMEOUT)
    put.raise_for_status()


def _fetch_blob(registry: str, oci_repo: str, digest: str, oci_token: str) -> bytes:
    """GET a blob's raw bytes (used to read prior-revision v2 pointer blobs)."""
    resp = _http.client().get(
        f"{registry}/v2/{oci_repo}/blobs/{digest}",
        headers={"Authorization": f"Bearer {oci_token}"},
        timeout=DEFAULT_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.content


def _build_dedup_index(existing, registry: str, oci_repo: str, oci_token: str) -> tuple:
    """From the prior revision's manifest, map already-stored chunks to their pack
    location and record each pack's size — the v2 "upload only new chunks" index.

    Returns `(chunk_index, pack_sizes)`: `chunk_index[chunk_digest] = (pack_digest,
    pack_offset)` for every chunk in a prior chunked-v2 file (fetched from its
    pointer blob), and `pack_sizes[pack_digest] = size` from the manifest's pack
    layers (so a reused pack can be re-listed with its size). A prior plain-blob
    revision contributes nothing — there are no packs to reuse.
    """
    chunk_index: Dict[str, tuple] = {}
    pack_sizes: Dict[str, int] = {}
    if existing is None:
        return chunk_index, pack_sizes
    manifest = existing.manifest
    for layer in manifest.get("layers", []):
        if layer.get("mediaType") == PACK_MEDIA_TYPE:
            pack_sizes[layer["digest"]] = layer["size"]
    # Fetch every prior chunked file's pointer blob CONCURRENTLY (independent,
    # small, read-only GETs) before the first new byte leaves the machine, then
    # merge in group order so the index stays deterministic (setdefault = first
    # wins). Uses the shared pooled httpx client, so the fan-out reuses warm
    # connections instead of re-handshaking per pointer.
    pointer_digests = [
        g.pointer_digest
        for g in group_files(manifest)
        if g.layout == CHUNKED_LAYOUT_V2 and g.pointer_digest is not None
    ]
    if pointer_digests:
        with ThreadPoolExecutor(max_workers=resolve_upload_workers()) as executor:
            blobs = list(executor.map(
                lambda pd: _fetch_blob(registry, oci_repo, pd, oci_token),
                pointer_digests,
            ))
        for blob in blobs:
            for ref in parse_pointer_v2(blob):
                chunk_index.setdefault(ref.chunk_digest, (ref.pack_digest, ref.pack_offset))
    return chunk_index, pack_sizes


# Process-wide bound on concurrent pack uploads. Shared across every file in a
# folder upload so the nested file×pack parallelism cannot multiply resident pack
# buffers past one ceiling. Rebuilt only when the configured cap changes (between
# top-level uploads), so a test can retune it via HIPPIUS_MAX_INFLIGHT_PACKS.
_pack_gate_lock = threading.Lock()
_pack_gate_state: Dict[str, object] = {"cap": None, "sem": None}


def _pack_upload_gate() -> threading.BoundedSemaphore:
    """Return the shared semaphore capping concurrent pack uploads (memory ceiling)."""
    cap = resolve_max_inflight_packs()
    with _pack_gate_lock:
        if _pack_gate_state["cap"] != cap:
            _pack_gate_state["cap"] = cap
            _pack_gate_state["sem"] = threading.BoundedSemaphore(cap)
        return _pack_gate_state["sem"]


def _upload_file_chunked_v2(
    abs_path: str,
    repo_title: str,
    file_size: int,
    registry: str,
    oci_repo: str,
    oci_token: str,
    dedup_index: Dict[str, tuple],
    pack_sizes: Dict[str, int],
) -> List[dict]:
    """Store a large file as a chunked-v2 pointer + ~64 MiB pack blobs.

    Chunks at the same 4 MiB CDC boundaries as v1, but chunks already present
    (per `dedup_index`) are referenced by range into their existing packs — only
    NEW chunks are packed and uploaded. The manifest lists the pointer plus every
    pack it references (new and reused) so each stays GC-safe. Pointer written
    last, like v1, so a crash leaves only unreferenced packs for GC."""
    whole_hex, chunk_metas = chunk_and_hash_native(abs_path, resolve_cdc_avg_size())
    chunks = [(f"sha256:{h}", size, offset) for h, offset, size in chunk_metas]
    plan = plan_packs(chunks, dedup_index, resolve_pack_size())
    uploads_url = f"{registry}/v2/{oci_repo}/blobs/uploads/"

    # Shared across all files: a thread blocks here BEFORE the native call
    # allocates the pack, so at most `resolve_max_inflight_packs()` packs are
    # resident at once regardless of how many files upload concurrently.
    gate = _pack_upload_gate()

    def _upload_pack(new_pack) -> str:
        with gate:
            hex_digest = pack_upload_native(
                uploads_url=uploads_url,
                path=abs_path,
                ranges=list(new_pack.ranges),
                auth_token=oci_token,
            )
        return f"sha256:{hex_digest}"

    # Packs are independent blobs → upload in parallel (the round-trip win: ~K/16
    # pack PUTs instead of K chunk PUTs). Order is preserved so digests line up
    # with plan.new_packs for resolve_pointer_chunks. The gate above caps the
    # cross-file total even though this pool is per-file.
    with ThreadPoolExecutor(max_workers=resolve_upload_workers()) as executor:
        new_pack_digests = list(executor.map(_upload_pack, plan.new_packs))

    pointer_chunks = resolve_pointer_chunks(plan, new_pack_digests)
    pointer_bytes = pointer_v2_bytes(whole_hex, file_size, pointer_chunks)
    pointer_digest = f"sha256:{hashlib.sha256(pointer_bytes).hexdigest()}"
    _ensure_bytes_blob_uploaded(registry, oci_repo, oci_token, pointer_bytes, pointer_digest)

    all_pack_sizes = dict(pack_sizes)
    for new_pack, digest in zip(plan.new_packs, new_pack_digests):
        all_pack_sizes[digest] = new_pack.size

    pointer_layer = {
        "mediaType": POINTER_MEDIA_TYPE_V2,
        "size": len(pointer_bytes),
        "digest": pointer_digest,
        "annotations": {
            LAYER_TITLE_KEY: repo_title.replace("\\", "/"),
            FILE_SIZE_KEY: str(file_size),
            FILE_DIGEST_KEY: f"sha256:{whole_hex}",
            CHUNK_COUNT_KEY: str(len(chunk_metas)),
        },
    }
    # One pack layer per referenced pack, in first-appearance order (deterministic).
    referenced: Dict[str, int] = {}
    for _cd, _sz, pack_digest, _off in pointer_chunks:
        if pack_digest not in referenced:
            try:
                referenced[pack_digest] = all_pack_sizes[pack_digest]
            except KeyError as exc:
                raise MalformedManifestError(
                    f"chunked-v2 references pack {pack_digest} with no known size "
                    "(prior manifest and pack layer are inconsistent)"
                ) from exc
    pack_layers = [
        {"mediaType": PACK_MEDIA_TYPE, "size": size, "digest": pd}
        for pd, size in referenced.items()
    ]
    return [pointer_layer, *pack_layers]


def _upload_file_layers(
    abs_path: str,
    repo_title: str,
    registry: str,
    oci_repo: str,
    oci_token: str,
    dedup_index: Optional[Dict[str, tuple]] = None,
    pack_sizes: Optional[Dict[str, int]] = None,
) -> List[dict]:
    """Upload one file and return its manifest layer(s).

    A file at or above the chunk threshold is stored as chunked-v2 (a pointer +
    ~64 MiB packs, reusing prior chunks by range via `dedup_index`) unless the
    rollout gate (`HIPPIUS_CHUNKED_WRITE=0`) forces plain uploads. Below the
    threshold, one plain blob — byte-identical to the pre-chunking layout, so
    small files cross-dedup."""
    file_size = os.path.getsize(abs_path)
    if file_size >= resolve_chunk_threshold() and resolve_chunked_write_enabled():
        return _upload_file_chunked_v2(
            abs_path, repo_title, file_size, registry, oci_repo, oci_token,
            dedup_index or {}, pack_sizes or {},
        )
    sha256_hash, size = hash_file_native(abs_path)
    if not _ensure_blob_uploaded(registry, oci_repo, oci_token, abs_path, sha256_hash):
        # Blob already present at its digest — the dedup HEAD hit. Surface the same
        # skip feedback the pre-chunking uploader emitted (dropped in the chunked
        # refactor); users and test_idempotency both read it as proof that "upload
        # only the bytes we're missing" is working.
        tqdm.write(f"✅ Already published (skipped): {repo_title}")
    return [_build_layer(sha256_hash, size, repo_title)]


def _prev_digest_or_warn(existing, repo_id: str, revision: str) -> Optional[str]:
    """Return the prior manifest's digest for If-Match, or None with a warning.

    If `existing` is None (fresh repo / 404 fetch), return None silently —
    there is no prior writer to race. If `existing.digest` is None (the
    registry honored the fetch but omitted Docker-Content-Digest), warn
    loudly: the next PUT will proceed without optimistic-concurrency
    protection, regressing this revision to last-writer-wins. Per OCI
    Distribution Spec §4.4.1, Docker-Content-Digest is RECOMMENDED but not
    REQUIRED on manifest responses, so some registries / stripping proxies
    legitimately omit it; we still want operators to see when they are
    running unprotected — silent regression is the failure mode the audit
    H1 fix was meant to close.
    """
    if existing is None:
        return None
    if existing.digest is None:
        warnings.warn(
            f"manifest fetch for {repo_id}:{revision} returned no "
            f"Docker-Content-Digest header; PUT will proceed without If-Match "
            f"and concurrent writers may silently overwrite each other",
            UserWarning,
            stacklevel=3,
        )
        return None
    return existing.digest


# Manifest-PUT resilience. Every blob a manifest references — packs, the pointer,
# the empty config — is uploaded (its PUT returned 2xx, all content-addressed)
# before we PUT the manifest listing them. But Harbor's S3-backed blob commit
# (the "Move" from the upload session into the blob store) has a measured
# multi-second window between accepting a blob (201) and making it visible to
# manifest validation: a just-PUT 8 MiB blob HEADs 404 for ~3.4s here. A fast
# client (a low-latency CI runner) can PUT the manifest inside that window and
# get an opaque 400 MANIFEST_BLOB_UNKNOWN; a slow (WAN) client never sees it,
# which is why this only ever reproduced on staging CI, never locally.
#
# The manifest bytes are deterministic and the revision-tag write is idempotent
# (content-addressed), so we retry, with bounded exponential backoff + jitter,
# the SAME set of transient conditions the Rust uploader retries (see
# `CoreError::is_retryable` in src/error.rs: connection/timeout errors + 408/429/
# 5xx — a plain 4xx is permanent, pinned by `upload_retry_skips_4xx`), PLUS one
# Harbor-manifest-specific case: a 400 whose OCI error code is (MANIFEST_)
# BLOB_UNKNOWN, i.e. the commit-visibility race above. Any OTHER 400 (a malformed
# or oversized manifest, MANIFEST_INVALID, a bad path/scope) is a real client
# error and fails fast — retrying it would only waste the backoff budget and
# mislabel the cause.
#
# Known edge (narrow, intentionally not handled here): on an *update* (a prior
# manifest exists, so If-Match is sent), if the registry commits our write but
# the response is lost to a retryable 5xx, the retry re-sends the now-stale
# If-Match and gets 412 → ConcurrentManifestUpdateError. The write in fact
# succeeded; re-running the upload (which re-fetches the digest and re-PUTs the
# identical, content-addressed manifest) resolves it. The blob-commit race — the
# case this fix targets — happens BEFORE the manifest is accepted, so If-Match is
# still valid on its retries.
MANIFEST_PUT_MAX_RETRIES = 5
_MANIFEST_PUT_BACKOFF_BASE_SECS = 0.5
_MANIFEST_PUT_BACKOFF_CAP_SECS = 8.0
# Transient statuses, matching the Rust uploader's is_retryable (408/429/5xx).
# 400 is handled separately (only the BLOB_UNKNOWN commit-race variant); 412 is a
# real concurrent-write conflict, surfaced typed and never retried.
_RETRYABLE_MANIFEST_STATUS = frozenset({408, 429, 500, 502, 503, 504})


def _manifest_error_detail(resp: httpx.Response) -> str:
    """Raw (truncated) response body from a failed manifest PUT, so the raised
    error carries Harbor's OCI error code (e.g. MANIFEST_BLOB_UNKNOWN) instead of
    an opaque status line. `raise_for_status` hides the body; this doesn't."""
    body = (resp.text or "").strip()
    return body[:500] if body else "(empty response body)"


def _is_blob_commit_race(resp: httpx.Response) -> bool:
    """True when a 400 carries Harbor's (MANIFEST_)BLOB_UNKNOWN OCI error code —
    the blob commit→visibility race (see the module note), the one 400 worth
    retrying. Substring-matches the raw body (no JSON parse, no try/except): the
    OCI error document is `{"errors":[{"code":"MANIFEST_BLOB_UNKNOWN",...}]}` and
    every such code contains `BLOB_UNKNOWN`."""
    return resp.status_code == 400 and "BLOB_UNKNOWN" in (resp.text or "")


def _put_manifest(
    registry: str,
    repo_id: str,
    revision: str,
    oci_token: str,
    manifest: dict,
    *,
    if_match: Optional[str] = None,
) -> httpx.Response:
    """PUT the manifest to revision. Returns the response (with digest in headers).

    When `if_match` is provided (the digest from a prior fetch_manifest call),
    sends `If-Match: <digest>` per OCI distribution spec 4.4. The server then
    rejects with 412 Precondition Failed if a concurrent writer has advanced
    the revision in between — we surface that as ConcurrentManifestUpdateError
    so callers can choose to retry or serialize externally rather than silently
    overwriting the other writer's layer.

    Transient conditions — a connection/timeout error, a 408/429/5xx, or the
    Harbor blob-commit-visibility 400 (see the module note) — are retried with
    bounded exponential backoff + jitter. Any other status fails fast.
    """
    url = f"{registry}/v2/{repo_id}/manifests/{revision}"
    headers = {
        "Authorization": f"Bearer {oci_token}",
        "Content-Type": "application/vnd.oci.image.manifest.v1+json",
    }
    if if_match:
        headers["If-Match"] = if_match

    resp = None
    for attempt in range(MANIFEST_PUT_MAX_RETRIES + 1):
        # A connection reset / timeout / protocol error mid-PUT (e.g. a Harbor
        # redeploy) is a transient transport failure — the same class the Rust
        # uploader retries. httpx raises it rather than returning a response, so
        # this narrow catch is load-bearing: retry it like a retryable status,
        # and re-raise once the attempts are spent.
        transport_error = None
        try:
            resp = _http.client().put(url, headers=headers, json=manifest, timeout=DEFAULT_HTTP_TIMEOUT * 2)
        except httpx.TransportError as exc:
            transport_error = exc

        if transport_error is None:
            if resp.status_code == 412:
                raise ConcurrentManifestUpdateError(
                    f"manifest at {repo_id}:{revision} changed between read and write",
                    response=resp,
                )
            if resp.is_success:
                return resp
            if (resp.status_code not in _RETRYABLE_MANIFEST_STATUS
                    and not _is_blob_commit_race(resp)):
                break
            if attempt == MANIFEST_PUT_MAX_RETRIES:
                break
            reason = ("blob-commit visibility lag" if _is_blob_commit_race(resp)
                      else f"transient {resp.status_code}")
        else:
            if attempt == MANIFEST_PUT_MAX_RETRIES:
                raise transport_error
            reason = f"transient network error ({type(transport_error).__name__})"

        backoff = min(_MANIFEST_PUT_BACKOFF_BASE_SECS * (2 ** attempt), _MANIFEST_PUT_BACKOFF_CAP_SECS)
        # Full jitter (50–100% of the computed delay) so independent uploaders
        # riding out the same registry blip don't retry in lockstep and re-storm
        # a recovering registry.
        delay = backoff * (0.5 + random.random() * 0.5)
        tqdm.write(
            f"⏳ Manifest PUT for {revision} — {reason}; retrying in {delay:.1f}s "
            f"[attempt {attempt + 1}/{MANIFEST_PUT_MAX_RETRIES + 1}]"
        )
        time.sleep(delay)

    # A BLOB_UNKNOWN that outlived the retry budget may be a blob we cache-skip (the
    # empty `{}` config), GC'd since we last confirmed it — not the transient
    # commit-visibility race the retries assume. Evict the config-blob cache entry so
    # a re-run re-confirms/re-PUTs it instead of skipping and failing identically.
    if resp is not None and _is_blob_commit_race(resp):
        _config_blob_present.discard((registry, repo_id))
    raise httpx.HTTPStatusError(
        f"manifest PUT for {repo_id}:{revision} failed with {resp.status_code} "
        f"after {attempt + 1} attempt(s): {_manifest_error_detail(resp)}",
        request=resp.request,
        response=resp,
    )


def _normalize_path_or_fileobj(path_or_fileobj) -> tuple:
    """Coerce HF's path_or_fileobj (str/Path/bytes/BinaryIO) into (filesystem_path, cleanup).
    cleanup() must be called after use; it's a no-op for real paths.

    Try/except here is load-bearing: a partial write (disk full, encoding error)
    must not leak a temp file. The caller pattern is `path, cleanup = ...; try: use(path); finally: cleanup()`.
    """
    if isinstance(path_or_fileobj, (str, Path)):
        return str(path_or_fileobj), lambda: None

    if not (isinstance(path_or_fileobj, bytes) or hasattr(path_or_fileobj, "read")):
        raise TypeError(
            f"path_or_fileobj must be str/Path/bytes/BinaryIO, got {type(path_or_fileobj).__name__}"
        )

    chunk_size = 4 * 1024 * 1024
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
    try:
        if isinstance(path_or_fileobj, bytes):
            tmp.write(path_or_fileobj)
        else:
            # Stream-read so multi-GB BinaryIO inputs don't materialize in memory.
            while True:
                chunk = path_or_fileobj.read(chunk_size)
                if not chunk:
                    break
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                tmp.write(chunk)
        tmp.flush()
    except BaseException:
        tmp.close()
        if os.path.exists(tmp.name):
            os.unlink(tmp.name)
        raise
    finally:
        if not tmp.closed:
            tmp.close()

    tmp_path = tmp.name
    return tmp_path, lambda: os.path.exists(tmp_path) and os.unlink(tmp_path)


def _build_layer(sha256_hash: str, file_size: int, path_in_repo: str) -> dict:
    return {
        "mediaType": "application/octet-stream",
        "size": file_size,
        "digest": f"sha256:{sha256_hash}",
        "annotations": {
            LAYER_TITLE_KEY: path_in_repo.replace("\\", "/"),
        },
    }


def _partition_groups(layers: List[dict]) -> List[tuple]:
    """Split a layer list into (title, [layers]) file-groups.

    A titled layer starts a group; untitled chunk layers attach to the preceding
    group. So a chunked file's pointer + K chunk layers are one indivisible unit:
    dropping it by title drops its chunks too, and keeping it keeps them all.
    This is what makes `_merge_layers` group-aware — the fix for the data-loss
    bug where a title-keyed merge either collapsed a chunked file to its pointer
    or wiped its chunk layers when an unrelated file was committed."""
    groups: List[tuple] = []
    for layer in layers:
        title = layer_title(layer)
        if title or not groups:
            groups.append((title, [layer]))
        else:
            groups[-1][1].append(layer)
    return groups


def _merge_layers(
    existing: List[dict],
    new_layers: List[dict],
    delete_titles: Optional[set] = None,
) -> List[dict]:
    """Combine `existing` with `new_layers` at file-group granularity.

    An existing file-group is dropped when its title is being replaced by
    `new_layers` or is in `delete_titles`; every surviving group is preserved
    INTACT (pointer + all its chunk layers), then the new layers are appended.
    A file's group is never partially rewritten, so committing one file can't
    damage another chunked file, and replacing a chunked file swaps its whole
    group. For plain single-layer files this reduces to the old title-keyed
    behavior (new replaces same-title, deletes drop, others preserved)."""
    delete_titles = delete_titles or set()
    new_titles = {title for title, _ in _partition_groups(new_layers) if title}
    result: List[dict] = []
    for title, group_layers in _partition_groups(existing):
        if title is not None and (title in delete_titles or title in new_titles):
            continue
        result.extend(group_layers)
    result.extend(new_layers)
    return result


def _assemble_manifest(
    config_digest: str,
    config_size: int,
    merged_layers: List[dict],
    commit_message: Optional[str],
    commit_description: Optional[str],
) -> dict:
    """Build the OCI manifest, typing it as a chunked artifact only when it holds
    at least one chunked file.

    A manifest with any pointer layer gets `artifactType` (so image tooling / Trivy
    treat it as a generic artifact, not a broken image) and the
    `com.hippius.layout` annotation (so a layout-blind client hits the Phase 0
    guard). A purely-plain manifest stays byte-identical to the pre-chunking
    output, preserving cross-dedup with existing artifacts."""
    annotations = _commit_annotations(commit_message, commit_description)
    manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": config_digest,
            "size": config_size,
        },
        "layers": merged_layers,
        "annotations": annotations,
    }
    # Tag the manifest chunked-v2 if any file in it uses the pack layout; the layout
    # annotation gates the whole manifest through the reader's forward-compat guard.
    if any(layer.get("mediaType") == POINTER_MEDIA_TYPE_V2 for layer in merged_layers):
        manifest["artifactType"] = ARTIFACT_TYPE_CHUNKED_V2
        annotations[LAYOUT_ANNOTATION_KEY] = CHUNKED_LAYOUT_V2
    _guard_manifest_size(manifest)
    return manifest


def _guard_manifest_size(manifest: dict) -> None:
    """Refuse a manifest that would exceed the registry's 4 MiB PUT cap.

    Checked here, before the blobs' manifest is PUT, so a too-large artifact
    fails with a clear error instead of the registry's opaque 400 after every
    blob is already uploaded. Serialized the same way httpx sends it (default
    `json.dumps` + utf-8) so the measured size matches the wire body."""
    size = len(json.dumps(manifest).encode("utf-8"))
    if size > MAX_MANIFEST_BYTES:
        raise ManifestTooLargeError(
            f"manifest is {size} bytes with {len(manifest['layers'])} layers, over the "
            f"{MAX_MANIFEST_BYTES}-byte registry limit; this artifact has too many chunks "
            "for a single manifest (Referrers/index fan-out is a planned follow-up)."
        )


def _commit_annotations(commit_message: Optional[str], commit_description: Optional[str]) -> dict:
    annotations = {
        "org.opencontainers.image.source": "hippius-hub",
        "org.opencontainers.image.created": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    if commit_message:
        annotations["org.hippius.commit.message"] = commit_message
    if commit_description:
        annotations["org.hippius.commit.description"] = commit_description
    return annotations


def _build_commit_info(
    registry: str,
    repo_id: str,
    revision: str,
    response: httpx.Response,
    commit_message: str,
    commit_description: str,
) -> CommitInfo:
    oid = response.headers.get("Docker-Content-Digest", "") or revision
    # HF's CommitInfo splits commit_url on "/commit/" and constructs a RepoUrl
    # from the prefix. Synthesize a URL whose prefix is parseable as a HF
    # "{namespace}/{repo_id}" URL relative to the registry endpoint.
    commit_url = f"{registry}/{repo_id}/commit/{oid}"
    return CommitInfo(
        commit_url=commit_url,
        commit_message=commit_message,
        commit_description=commit_description,
        oid=oid,
        _endpoint=registry,
    )


def _upload_one_file(
    *,
    rel_path: str,
    base_dir: str,
    path_in_repo: Optional[str],
    registry: str,
    oci_repo: str,
    oci_token: str,
    dedup_index: Optional[Dict[str, tuple]] = None,
    pack_sizes: Optional[Dict[str, int]] = None,
) -> List[dict]:
    """Upload one file from a folder and return its manifest layer(s).

    Returns a LIST because a chunked file contributes a pointer layer plus K
    chunk layers (a plain file contributes one). Extracted from the per-file
    closure in `upload_folder` so the thread-pool body is testable in isolation.

    `dedup_index`/`pack_sizes` come from the prior revision (built once by
    `upload_folder`) so a chunked file references already-stored chunks by range
    instead of re-uploading them; empty/None for plain uploads.
    """
    abs_path = os.path.join(base_dir, rel_path)
    repo_title = f"{path_in_repo}/{rel_path}" if path_in_repo else rel_path
    tqdm.write(f"🚀 Uploading: {repo_title} ({os.path.getsize(abs_path)} bytes)...")
    layers = _upload_file_layers(
        abs_path, repo_title, registry, oci_repo, oci_token, dedup_index, pack_sizes
    )
    tqdm.write(f"✅ Uploaded: {repo_title}")
    return layers


def _finalize_upload_manifest(
    *,
    registry: str,
    oci_repo: str,
    oci_token: str,
    repo_id: str,
    revision: str,
    new_layers: List[dict],
    delete_patterns: Optional[Union[List[str], str]],
    commit_message: str,
    commit_description: str,
) -> CommitInfo:
    """Merge new layers into the existing manifest and PUT it (with If-Match).

    Single read-modify-write on the manifest: one fetch reused for both the
    delete-title computation and the merge, the captured digest threaded back
    into the PUT as `If-Match` so a racing writer surfaces as 412 →
    `ConcurrentManifestUpdateError` rather than silent last-writer-wins. The
    deliberate non-reuse from `upload_file` is documented on `_upload_one_file`.
    """
    existing = fetch_manifest(registry, oci_repo, revision, oci_token, missing_ok=True)
    existing_layers = existing.manifest.get("layers", []) if existing else []
    prev_digest = _prev_digest_or_warn(existing, repo_id, revision)

    delete_titles = set()
    if delete_patterns:
        existing_titles = [t for t in (layer_title(l) for l in existing_layers) if t]
        delete_titles = set(filter_repo_objects(items=existing_titles, allow_patterns=delete_patterns))

    merged_layers = _merge_layers(existing_layers, new_layers, delete_titles=delete_titles)

    config_digest, config_size = _ensure_config_blob_uploaded(registry, oci_repo, oci_token)
    manifest = _assemble_manifest(
        config_digest, config_size, merged_layers, commit_message, commit_description
    )

    print(f"📝 Publishing OCI Manifest for {revision}...")
    resp = _put_manifest(registry, oci_repo, revision, oci_token, manifest, if_match=prev_digest)
    return _build_commit_info(registry, repo_id, revision, resp, commit_message, commit_description)


def _handle_unsupported_kwargs(create_pr, parent_commit, run_as_future):
    """`create_pr` and `parent_commit` are accept-and-warn (closest HF analog is
    "no PR concept" and "no optimistic concurrency" — the upload still proceeds).
    `run_as_future` would require returning a Future we can't fulfill — raise."""
    if create_pr:
        warnings.warn(
            "create_pr=True is ignored: Hippius has no pull-request concept; "
            "the upload writes directly to the revision.",
            UserWarning,
            stacklevel=3,
        )
    if parent_commit:
        warnings.warn(
            "parent_commit is ignored: Hippius revisions are OCI tags without "
            "an HF-style optimistic-concurrency check.",
            UserWarning,
            stacklevel=3,
        )
    if run_as_future:
        raise NotImplementedError("run_as_future is not yet supported by hippius_hub")


# ---- public API ----

def upload_file(
    *,
    path_or_fileobj: Union[str, Path, bytes, BinaryIO],
    path_in_repo: str,
    repo_id: str,
    token: Union[bool, str, None] = None,
    repo_type: Optional[str] = None,
    revision: Optional[str] = None,
    commit_message: Optional[str] = None,
    commit_description: Optional[str] = None,
    create_pr: Optional[bool] = None,
    parent_commit: Optional[str] = None,
    run_as_future: bool = False,
    endpoint: Optional[str] = None,
) -> CommitInfo:
    """Upload a single file to a repository revision and return a CommitInfo.

    Merges with the existing manifest: any layer with the same title is replaced.
    bytes / file-like objects are written to a temp file before hashing.

    Concurrency: this is a read-modify-write on the manifest. We send
    `If-Match: <previous-digest>` on the PUT so the registry rejects (412 →
    `ConcurrentManifestUpdateError`) when a racing writer has advanced the
    revision between our fetch and our PUT — the alternative (silent
    last-writer-wins) loses the racing writer's layer. Callers receive the
    typed exception and can retry or serialize externally.
    """
    _validate_repo_type(repo_type)
    _handle_unsupported_kwargs(create_pr, parent_commit, run_as_future)
    if revision is None:
        revision = "main"
    if commit_message is None:
        commit_message = f"Upload {path_in_repo}"
    if commit_description is None:
        commit_description = ""

    oci_repo = _oci_repo_path(repo_id, repo_type)
    registry = resolve_registry(endpoint)
    oci_token = _oci_bearer(oci_repo, token, push=True, endpoint=endpoint)

    # Fetch the prior manifest BEFORE uploading: chunked-v2 needs it to build the
    # dedup index (which chunks already exist, so only new chunks are packed). The
    # same digest guards the PUT via If-Match, so moving the fetch earlier does not
    # widen the concurrency window.
    existing = fetch_manifest(registry, oci_repo, revision, oci_token, missing_ok=True)

    file_path, cleanup = _normalize_path_or_fileobj(path_or_fileobj)
    try:
        dedup_index: Dict[str, tuple] = {}
        pack_sizes: Dict[str, int] = {}
        # Only build the dedup index (a pointer-blob GET fan-out over the prior
        # revision) when THIS file is large enough to take the chunked-v2 path
        # (audit L-DEDUP-EARLY). A sub-threshold file uploads as one plain blob and
        # never reads the index, so building it would be pure wasted round-trips.
        if resolve_chunked_write_enabled() and (
            os.path.getsize(file_path) >= resolve_chunk_threshold()
        ):
            dedup_index, pack_sizes = _build_dedup_index(existing, registry, oci_repo, oci_token)
        new_layers = _upload_file_layers(
            file_path, path_in_repo, registry, oci_repo, oci_token, dedup_index, pack_sizes
        )
    finally:
        cleanup()

    existing_layers = existing.manifest.get("layers", []) if existing else []
    prev_digest = _prev_digest_or_warn(existing, repo_id, revision)
    merged_layers = _merge_layers(existing_layers, new_layers)

    config_digest, config_size = _ensure_config_blob_uploaded(registry, oci_repo, oci_token)
    manifest = _assemble_manifest(
        config_digest, config_size, merged_layers, commit_message, commit_description
    )

    resp = _put_manifest(registry, oci_repo, revision, oci_token, manifest, if_match=prev_digest)
    return _build_commit_info(registry, repo_id, revision, resp, commit_message, commit_description)


def upload_folder(
    *,
    repo_id: str,
    folder_path: Union[str, Path],
    path_in_repo: Optional[str] = None,
    commit_message: Optional[str] = None,
    commit_description: Optional[str] = None,
    token: Union[bool, str, None] = None,
    repo_type: Optional[str] = None,
    revision: Optional[str] = None,
    create_pr: Optional[bool] = None,
    parent_commit: Optional[str] = None,
    allow_patterns: Optional[Union[List[str], str]] = None,
    ignore_patterns: Optional[Union[List[str], str]] = None,
    delete_patterns: Optional[Union[List[str], str]] = None,
    run_as_future: bool = False,
    endpoint: Optional[str] = None,
    max_workers: int = 8,
) -> CommitInfo:
    """Upload every file under `folder_path` to a repository revision.

    Honors HF allow_patterns/ignore_patterns/delete_patterns. Merges with the
    existing manifest — any layer with a matching title is replaced; titles
    matching delete_patterns are removed from the new manifest entirely.

    Concurrency: like `upload_file`, the PUT carries `If-Match` from the
    manifest fetch. A concurrent writer that advanced the revision in the
    meantime causes the registry to return 412, which surfaces here as
    `ConcurrentManifestUpdateError` — the partial-folder write does NOT
    silently land. Blob pushes that already completed are idempotent at the
    OCI level (content-addressed), so a retry of the whole folder is safe.

    `max_workers` controls the per-file ThreadPoolExecutor — mirrors the
    parameter on `snapshot_download` so callers with large folders and fast
    pipes can push the upload pool past the default of 8.
    """
    _validate_repo_type(repo_type)
    _handle_unsupported_kwargs(create_pr, parent_commit, run_as_future)
    if revision is None:
        revision = "main"
    if commit_message is None:
        commit_message = "Upload folder using hippius_hub"
    if commit_description is None:
        commit_description = ""

    base_dir = os.path.abspath(str(folder_path))
    if not os.path.isdir(base_dir):
        raise ValueError(f"folder_path {folder_path!r} is not a directory")

    all_relative = []
    for root, _, files in os.walk(base_dir):
        for fname in files:
            rel = os.path.relpath(os.path.join(root, fname), base_dir).replace("\\", "/")
            all_relative.append(rel)

    filtered = list(filter_repo_objects(
        items=all_relative,
        allow_patterns=allow_patterns,
        ignore_patterns=ignore_patterns,
    ))

    oci_repo = _oci_repo_path(repo_id, repo_type)
    registry = resolve_registry(endpoint)
    oci_token = _oci_bearer(oci_repo, token, push=True, endpoint=endpoint)

    # Build the chunked-v2 dedup index ONCE from the prior revision (as upload_file
    # does) so every large file in the folder references chunks already stored
    # instead of re-packing and re-uploading them. Read-only and taken before the
    # fan-out; _finalize_upload_manifest re-fetches for the merge + If-Match, so
    # this does not widen that write's read-modify-write window. Only built under
    # HIPPIUS_CHUNKED_WRITE — plain uploads dedup per-blob via HEAD.
    dedup_index: Dict[str, tuple] = {}
    pack_sizes: Dict[str, int] = {}
    if resolve_chunked_write_enabled():
        # Only build the dedup index (a manifest GET + N pointer-blob GETs) when at
        # least one file will actually be chunked (audit L10) — otherwise a folder
        # of only-small files pays ~2N+1 wasted round-trips before the first byte,
        # for a plain-path upload that never consults the index. Mirrors upload_file.
        threshold = resolve_chunk_threshold()
        if any(os.path.getsize(os.path.join(base_dir, rel)) >= threshold for rel in filtered):
            prior = fetch_manifest(registry, oci_repo, revision, oci_token, missing_ok=True)
            dedup_index, pack_sizes = _build_dedup_index(prior, registry, oci_repo, oci_token)

    new_layers = []
    if filtered:
        print(f"📦 Preparing to upload {len(filtered)} file(s) to {repo_id}:{revision}...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    _upload_one_file,
                    rel_path=rel,
                    base_dir=base_dir,
                    path_in_repo=path_in_repo,
                    registry=registry,
                    oci_repo=oci_repo,
                    oci_token=oci_token,
                    dedup_index=dedup_index,
                    pack_sizes=pack_sizes,
                )
                for rel in filtered
            ]
            try:
                for fut in tqdm(as_completed(futures), total=len(filtered), desc="Uploading", unit="file"):
                    new_layers.extend(fut.result())
            except BaseException:
                # Fail-fast (audit M3): mirror _snapshot_download — drop queued
                # uploads on the first failure / Ctrl-C. Blob PUTs are idempotent
                # and the manifest PUT is deferred to after all files succeed, so a
                # partial run leaves only orphan content-addressed blobs a GC
                # reclaims — never a bad manifest.
                executor.shutdown(wait=False, cancel_futures=True)
                raise

    commit_info = _finalize_upload_manifest(
        registry=registry,
        oci_repo=oci_repo,
        oci_token=oci_token,
        repo_id=repo_id,
        revision=revision,
        new_layers=new_layers,
        delete_patterns=delete_patterns,
        commit_message=commit_message,
        commit_description=commit_description,
    )
    # Count logical files uploaded, not manifest layers: a chunked file expands
    # to a pointer + K chunk layers, so len(new_layers) would overcount.
    print(f"🎉 Successfully pushed {len(filtered)} file(s) to {repo_id}:{revision}")
    return commit_info


def hippius_hub_upload(
    repo_id: str,
    local_path: str,
    revision: Optional[str] = "main",
    token: Optional[str] = None,
) -> CommitInfo:
    """Backward-compatible alias: dispatches to upload_file or upload_folder."""
    if os.path.isfile(local_path):
        return upload_file(
            path_or_fileobj=local_path,
            path_in_repo=os.path.basename(local_path),
            repo_id=repo_id,
            token=token,
            revision=revision,
        )
    return upload_folder(
        repo_id=repo_id,
        folder_path=local_path,
        token=token,
        revision=revision,
    )
