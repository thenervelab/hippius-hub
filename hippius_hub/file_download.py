import os
import shutil
import tempfile
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Union

from ._oci import fetch_manifest, layer_title
from .auth import get_oci_bearer_token, get_token, resolve_token_value
from .constants import DEFAULT_CACHE_DIR, resolve_registry
from .errors import (
    EntryNotFoundError,
    LocalEntryNotFoundError,
    RevisionNotFoundError,
)

try:
    from .hippius_core import download_file_native
except ImportError:
    raise ImportError("hippius_core is not installed. Did you run `maturin develop`?")


_VALID_REPO_TYPES = (None, "model", "dataset", "space")
_DEFAULT_CHUNK_SIZE = 100 * 1024 * 1024


def _resolve_chunk_size() -> int:
    raw = os.environ.get("HIPPIUS_CHUNK_SIZE")
    if not raw:
        return _DEFAULT_CHUNK_SIZE
    size = int(raw)
    if size <= 0:
        raise ValueError(f"HIPPIUS_CHUNK_SIZE must be a positive integer, got {size}")
    return size


def _resolve_verify_hash() -> bool:
    raw = os.environ.get("HIPPIUS_VERIFY_HASH", "").lower()
    return raw in ("1", "true", "yes")


def _validate_repo_type(repo_type: Optional[str]):
    if repo_type not in _VALID_REPO_TYPES:
        raise NotImplementedError(
            f"repo_type={repo_type!r} is not supported by hippius_hub. "
            f"Valid values: {', '.join(repr(t) for t in _VALID_REPO_TYPES)}."
        )


def _handle_ignored_download_kwargs(
    *,
    etag_timeout: float,
    tqdm_class,
    dry_run: bool,
    headers,
    user_agent,
    library_name,
    library_version,
):
    """Emit UserWarning for HF kwargs we accept but don't yet honor.

    Use only from hf_hub_download. snapshot_download has its own variant
    (it implements dry_run, so doesn't raise for it). stacklevel=3 points
    the warning at the user's call site, not this helper.
    """
    # Compare against the documented HF default rather than truthiness:
    # etag_timeout=0 is a valid (if odd) user choice that should still warn.
    if etag_timeout != 10.0:
        warnings.warn(
            "etag_timeout is ignored: hippius_hub does not perform ETag negotiation.",
            UserWarning,
            stacklevel=3,
        )
    if tqdm_class is not None:
        warnings.warn(
            "tqdm_class is ignored: hippius_hub uses its own progress bar.",
            UserWarning,
            stacklevel=3,
        )
    if dry_run:
        # dry_run is supported in snapshot_download but NOT here — fail fast
        # so users don't silently get a full download when they asked for a
        # no-op enumeration.
        raise NotImplementedError(
            "dry_run is not supported by hf_hub_download; use snapshot_download(dry_run=True) "
            "to enumerate files without downloading."
        )
    if headers:
        warnings.warn(
            "headers= is ignored: hippius_hub doesn't pass custom HTTP headers yet.",
            UserWarning,
            stacklevel=3,
        )
    if user_agent:
        warnings.warn("user_agent is ignored.", UserWarning, stacklevel=3)
    if library_name or library_version:
        warnings.warn(
            "library_name/library_version are ignored.",
            UserWarning,
            stacklevel=3,
        )


def _oci_repo_path(repo_id: str, repo_type: Optional[str]) -> str:
    """Translate (repo_id, repo_type) into the OCI-side repo path under `/v2/...`.

    Models keep their repo_id as-is (back-compat). Datasets and spaces are
    namespaced under the corresponding Harbor project so the same logical
    repo_id can exist across types without collision.

    Rejects already-prefixed repo_ids (`datasets/foo` with `repo_type="dataset"`)
    rather than producing a double-prefixed `datasets/datasets/foo` path.
    """
    if repo_type in (None, "model"):
        return repo_id
    if repo_type in ("dataset", "space"):
        prefix = f"{repo_type}s"  # datasets, spaces
        if repo_id.startswith(f"{prefix}/"):
            raise ValueError(
                f"repo_id={repo_id!r} already starts with {prefix + '/'!r}; "
                f"pass repo_id without the prefix when using repo_type={repo_type!r}"
            )
        return f"{prefix}/{repo_id}"
    # _validate_repo_type guards the only inputs that could reach here.
    # AssertionError (not NotImplementedError) is the canonical "unreachable
    # by design" signal — NotImplementedError reads as "TODO: implement",
    # which would be misleading: there is nothing left to implement here.
    raise AssertionError(
        f"unreachable: _validate_repo_type should have rejected {repo_type!r} before this point"
    )


def _cache_dirname(repo_id: str, repo_type: Optional[str]) -> str:
    """HF cache convention: `{kind}s--{namespace}--{name}` per repo type."""
    prefix = "models" if repo_type in (None, "model") else f"{repo_type}s"
    return f"{prefix}--{repo_id.replace('/', '--')}"


@dataclass(frozen=True)
class _DownloadPaths:
    """Cache-layout-resolved paths for a single file in hf_hub_download.

    `dest_file` is the final on-disk path of the downloaded file. When
    `local_dir` is set, only `dest_file` is meaningful (snapshots/repo_dir
    are not used). Otherwise the snapshots layout is populated for the
    cache.
    """

    dest_file: str
    # The next two are None when local_dir is set (we don't use cache layout).
    repo_dir: Optional[str]
    snapshots_dir: Optional[str]


def _resolve_dest_paths(
    *,
    repo_id: str,
    filename: str,
    repo_type: Optional[str],
    revision: str,
    cache_dir: str,
    local_dir: Optional[Union[str, Path]],
) -> _DownloadPaths:
    """Compute where this file lands on disk given (local_dir vs cache_dir)."""
    if local_dir is not None:
        return _DownloadPaths(
            dest_file=os.path.join(str(local_dir), filename),
            repo_dir=None,
            snapshots_dir=None,
        )
    repo_dir = os.path.join(cache_dir, _cache_dirname(repo_id, repo_type))
    snapshots_dir = os.path.join(repo_dir, "snapshots", revision)
    dest_file = os.path.join(snapshots_dir, filename)
    return _DownloadPaths(
        dest_file=dest_file, repo_dir=repo_dir, snapshots_dir=snapshots_dir
    )


def _resolve_target_digest(
    manifest: Dict,
    filename: str,
    repo_id: str,
    revision: str,
) -> str:
    """Find the layer whose title matches `filename` and return its digest.

    Raises EntryNotFoundError if no layer matches OR if a matching layer has
    no digest — preserves the exact behavior of the inline `if not target_digest`
    fall-through that previously lived in hf_hub_download (both the "no match"
    and "matched but digest is falsy" cases produced the same error message).
    """
    for layer in manifest.get("layers", []):
        if layer_title(layer) == filename:
            digest = layer.get("digest")
            if digest:
                return digest
            break
    raise EntryNotFoundError(
        f"File '{filename}' not found in the OCI manifest of '{repo_id}:{revision}'"
    )


def _resolve_manifest(
    *,
    registry: str,
    oci_repo: str,
    revision: str,
    oci_token: str,
    cached: Optional[Dict],
) -> Dict:
    """Return the caller-provided manifest, or fetch it from the registry.

    `cached` is the `_resolved_manifest` kwarg of hf_hub_download — populated
    by snapshot_download to avoid N+1 manifest round-trips when downloading
    many files from the same repo:revision. Read path only: no If-Match.
    """
    if cached is not None:
        return cached
    return fetch_manifest(registry, oci_repo, revision, oci_token).manifest


def hf_hub_download(
    repo_id: str,
    filename: str,
    *,
    subfolder: Optional[str] = None,
    repo_type: Optional[str] = None,
    revision: Optional[str] = None,
    library_name: Optional[str] = None,
    library_version: Optional[str] = None,
    cache_dir: Optional[Union[str, Path]] = None,
    local_dir: Optional[Union[str, Path]] = None,
    user_agent: Union[Dict, str, None] = None,
    force_download: bool = False,
    etag_timeout: float = 10.0,
    token: Union[bool, str, None] = None,
    local_files_only: bool = False,
    headers: Optional[Dict[str, str]] = None,
    endpoint: Optional[str] = None,
    tqdm_class: Optional[type] = None,
    dry_run: bool = False,
    _resolved_manifest: Optional[Dict] = None,
    _oci_token: Optional[str] = None,
) -> str:
    """Drop-in replacement for huggingface_hub.hf_hub_download against an
    OCI-backed Hippius registry.

    Honored kwargs: subfolder, revision, cache_dir, local_dir, force_download,
    token, local_files_only, endpoint, repo_type (model-only).
    Accepted but currently ignored (UserWarning at call site):
        etag_timeout (when != 10.0), tqdm_class, headers, user_agent,
        library_name, library_version.
    Rejected (raises NotImplementedError): dry_run — supported in
        snapshot_download but not here, raise rather than silently download.

    hippius_hub-specific overrides via env: HIPPIUS_CHUNK_SIZE, HIPPIUS_VERIFY_HASH.
    """
    _validate_repo_type(repo_type)
    _handle_ignored_download_kwargs(
        etag_timeout=etag_timeout,
        tqdm_class=tqdm_class,
        dry_run=dry_run,
        headers=headers,
        user_agent=user_agent,
        library_name=library_name,
        library_version=library_version,
    )
    if force_download and local_files_only:
        raise ValueError(
            "Cannot pass 'force_download=True' and 'local_files_only=True' at the same time."
        )
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR
    cache_dir = str(cache_dir)
    if revision is None:
        revision = "main"
    if subfolder:
        filename = f"{subfolder}/{filename}"

    oci_repo = _oci_repo_path(repo_id, repo_type)
    paths = _resolve_dest_paths(
        repo_id=repo_id,
        filename=filename,
        repo_type=repo_type,
        revision=revision,
        cache_dir=cache_dir,
        local_dir=local_dir,
    )
    # 1. Cache check: never redownload an existing file
    if not force_download and os.path.exists(paths.dest_file):
        return paths.dest_file
    if local_files_only:
        raise LocalEntryNotFoundError(
            f"{filename!r} not found in local cache (cache_dir={cache_dir!r}) "
            f"and local_files_only=True"
        )

    registry = resolve_registry(endpoint)
    auth_token = resolve_token_value(token)
    # _oci_token / _resolved_manifest let snapshot_download avoid N+1 round-trips.
    oci_token = _oci_token or get_oci_bearer_token(oci_repo, auth_token)
    manifest = _resolve_manifest(
        registry=registry,
        oci_repo=oci_repo,
        revision=revision,
        oci_token=oci_token,
        cached=_resolved_manifest,
    )
    target_digest = _resolve_target_digest(manifest, filename, repo_id, revision)
    blob_url = f"{registry}/v2/{oci_repo}/blobs/{target_digest}"
    if local_dir is not None:
        return _download_to_local_dir(blob_url, paths.dest_file, oci_token)
    return _download_to_cache(
        blob_url=blob_url,
        repo_dir=paths.repo_dir,
        snapshots_dir=paths.snapshots_dir,
        filename=filename,
        oci_token=oci_token,
        target_digest=target_digest,
    )


def _download_to_cache(
    blob_url, repo_dir, snapshots_dir, filename, oci_token, target_digest
):
    """Cache-structured download mirroring huggingface_hub's layout."""
    # Cache layout modeled on huggingface_hub
    blobs_dir = os.path.join(repo_dir, "blobs")
    os.makedirs(blobs_dir, exist_ok=True)
    os.makedirs(snapshots_dir, exist_ok=True)

    file_path = os.path.join(snapshots_dir, filename)
    # Unique per call: two concurrent downloaders writing the same logical
    # file no longer race on a shared temp path. mkstemp returns (fd, path);
    # we close the fd immediately because the Rust download_file_native
    # opens the file by path, not by inherited handle.
    safe_name = filename.replace("/", "_")
    fd, temp_path = tempfile.mkstemp(
        dir=blobs_dir,
        prefix=f"tmp_{safe_name}_",
    )
    os.close(fd)

    # 2. Concurrent download via the Rust engine
    print(f"Downloading {filename} (parallel)...")
    verify_hash = _resolve_verify_hash()
    try:
        # Audit L6 (Phase 3.12): download_file_native now returns
        # Optional[str] — `None` when verify_hash=False (skipped), `str`
        # when verify_hash=True. Previously it returned `""` as an
        # in-band sentinel for "skipped", which forced callers to know
        # the convention and would collide with any legitimate empty
        # string. The dispatch below is on `is not None`, not on
        # `verify_hash`, so a future code path that returns `None` for
        # other reasons (e.g. async cancellation) routes through the
        # same fallback rather than masquerading as a valid digest.
        calculated_hash = download_file_native(
            url=blob_url,
            dest_path=temp_path,
            auth_token=oci_token,
            chunk_size=_resolve_chunk_size(),
            verify_hash=verify_hash,
        )
    except Exception:
        # Clean up the mkstemp file before bubbling up. The inner OSError
        # swallow is intentional: a cleanup failure (file already gone,
        # permissions) must not shadow the original download exception.
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise

    # If hash verification was skipped (calculated_hash is None), fall
    # back to the digest from the OCI manifest. Otherwise use what the
    # Rust side computed.
    final_hash = (
        calculated_hash
        if calculated_hash is not None
        else target_digest.replace("sha256:", "")
    )

    # 3. Atomic rename of the temp file into the SHA256 blob
    blob_path = os.path.join(blobs_dir, f"sha256:{final_hash}")
    if not os.path.exists(blob_path):
        os.rename(temp_path, blob_path)
    elif os.path.exists(temp_path):
        os.remove(temp_path)

    # 4. Create the symlink in the snapshot
    _create_symlink(blob_path, file_path)
    return file_path


def _download_to_local_dir(blob_url, dest_file, oci_token):
    """Direct download to a user-chosen directory — bypasses the cache layout."""
    parent = os.path.dirname(dest_file)
    if parent:
        os.makedirs(parent, exist_ok=True)
    print(f"Downloading {os.path.basename(dest_file)} (parallel)...")
    # local_dir mode writes directly to the user-chosen path and does
    # not assemble a content-addressed blob layout, so the calculated
    # hash (Optional[str] post Phase 3.12) is intentionally unused — the
    # file path is the identity here, not the digest.
    download_file_native(
        url=blob_url,
        dest_path=dest_file,
        auth_token=oci_token,
        chunk_size=_resolve_chunk_size(),
        verify_hash=_resolve_verify_hash(),
    )
    return dest_file


def _create_symlink(src: str, dst: str) -> None:
    """Materialize the snapshot entry at `dst` pointing at the blob at `src`.

    Prefers a symlink (cheapest, allows cross-volume cache); falls back to
    a hardlink (same-volume only); falls back to a full copy (doubles disk
    use). Each fallback emits a UserWarning so operators see when the cache
    is more expensive than expected — Windows without developer mode,
    sandboxed CI without symlink capability, and removable FAT/exFAT drives
    are the common offenders.
    """
    if os.path.exists(dst):
        os.remove(dst)

    os.makedirs(os.path.dirname(dst), exist_ok=True)

    try:
        # Relative path from the snapshot directory to the blob so the cache
        # remains portable if the cache root is moved (the link target stays
        # valid as long as the relative layout is preserved).
        rel_src = os.path.relpath(src, os.path.dirname(dst))
        os.symlink(rel_src, dst)
        return
    except OSError as e:
        warnings.warn(
            f"symlink {src!r} -> {dst!r} failed ({e}); falling back to hardlink",
            UserWarning,
            stacklevel=2,
        )

    try:
        os.link(src, dst)
        return
    except OSError as e:
        warnings.warn(
            f"hardlink {src!r} -> {dst!r} failed ({e}); falling back to full copy "
            f"-- this doubles disk usage for the snapshot",
            UserWarning,
            stacklevel=2,
        )

    shutil.copy2(src, dst)


def try_to_load_from_cache(
    repo_id: str,
    filename: str,
    cache_dir: Optional[Union[str, Path]] = None,
    revision: Optional[str] = None,
    repo_type: Optional[str] = None,
) -> Optional[str]:
    """Return the cached file path if present, or None otherwise.

    Pure local filesystem check — never hits the network. Mirrors HF's
    behavior modulo the _CACHED_NO_EXIST sentinel, which we never return
    (we don't track known-404 entries separately).
    """
    _validate_repo_type(repo_type)
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR
    if revision is None:
        revision = "main"

    file_path = os.path.join(
        str(cache_dir),
        _cache_dirname(repo_id, repo_type),
        "snapshots",
        revision,
        filename,
    )
    if os.path.exists(file_path):
        return file_path
    return None


def hf_hub_url(
    repo_id: str,
    filename: str,
    *,
    subfolder: Optional[str] = None,
    repo_type: Optional[str] = None,
    revision: Optional[str] = None,
    endpoint: Optional[str] = None,
) -> str:
    """Return an OCI manifest URL for the file.

    Unlike huggingface_hub.hf_hub_url which returns a direct CDN download
    URL, this returns the OCI manifest URL — OCI blobs are content-addressed
    and require resolving the digest from the manifest first. For actual
    downloads use hf_hub_download.
    """
    _validate_repo_type(repo_type)
    if subfolder:
        filename = f"{subfolder}/{filename}"
    base = resolve_registry(endpoint)
    rev = revision or "main"
    return f"{base}/v2/{_oci_repo_path(repo_id, repo_type)}/manifests/{rev}"


hippius_hub_download = hf_hub_download
