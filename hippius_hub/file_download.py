import os
import shutil
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
    # Already validated by _validate_repo_type; unreachable.
    raise NotImplementedError(f"repo_type={repo_type!r}")


def _cache_dirname(repo_id: str, repo_type: Optional[str]) -> str:
    """HF cache convention: `{kind}s--{namespace}--{name}` per repo type."""
    prefix = "models" if repo_type in (None, "model") else f"{repo_type}s"
    return f"{prefix}--{repo_id.replace('/', '--')}"


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
    Accepted but currently ignored: etag_timeout, tqdm_class, dry_run, headers.

    hippius_hub-specific overrides via env: HIPPIUS_CHUNK_SIZE, HIPPIUS_VERIFY_HASH.
    """
    _validate_repo_type(repo_type)
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

    if local_dir is not None:
        dest_file = os.path.join(str(local_dir), filename)
    else:
        repo_dir = os.path.join(cache_dir, _cache_dirname(repo_id, repo_type))
        snapshots_dir = os.path.join(repo_dir, "snapshots", revision)
        dest_file = os.path.join(snapshots_dir, filename)

    # 1. Cache check: never redownload an existing file
    if not force_download and os.path.exists(dest_file):
        return dest_file

    if local_files_only:
        raise LocalEntryNotFoundError(
            f"{filename!r} not found in local cache (cache_dir={cache_dir!r}) "
            f"and local_files_only=True"
        )

    registry = resolve_registry(endpoint)
    auth_token = resolve_token_value(token)
    # _oci_token + _resolved_manifest are internal kwargs used by
    # snapshot_download to avoid N+1 token/manifest round-trips when
    # downloading many files from the same repo:revision.
    oci_token = _oci_token or get_oci_bearer_token(oci_repo, auth_token)

    if _resolved_manifest is not None:
        manifest = _resolved_manifest
    else:
        # Fetch the OCI manifest to find the file's exact digest. Read path:
        # we only need the body, not the digest — there's no PUT to thread
        # If-Match into here.
        manifest = fetch_manifest(registry, oci_repo, revision, oci_token).manifest

    target_digest = None
    for layer in manifest.get("layers", []):
        if layer_title(layer) == filename:
            target_digest = layer.get("digest")
            break

    if not target_digest:
        raise EntryNotFoundError(
            f"File '{filename}' not found in the OCI manifest of '{repo_id}:{revision}'"
        )

    blob_url = f"{registry}/v2/{oci_repo}/blobs/{target_digest}"

    if local_dir is not None:
        return _download_to_local_dir(blob_url, dest_file, oci_token)

    return _download_to_cache(
        blob_url=blob_url,
        repo_dir=repo_dir,
        snapshots_dir=snapshots_dir,
        filename=filename,
        oci_token=oci_token,
        target_digest=target_digest,
    )


def _download_to_cache(blob_url, repo_dir, snapshots_dir, filename, oci_token, target_digest):
    """Cache-structured download mirroring huggingface_hub's layout."""
    # Cache layout modeled on huggingface_hub
    blobs_dir = os.path.join(repo_dir, "blobs")
    os.makedirs(blobs_dir, exist_ok=True)
    os.makedirs(snapshots_dir, exist_ok=True)

    file_path = os.path.join(snapshots_dir, filename)
    temp_path = os.path.join(blobs_dir, f"tmp_{filename.replace('/', '_')}")

    # 2. Concurrent download via the Rust engine
    print(f"Downloading {filename} (parallel)...")
    verify_hash = _resolve_verify_hash()
    calculated_hash = download_file_native(
        url=blob_url,
        dest_path=temp_path,
        auth_token=oci_token,
        chunk_size=_resolve_chunk_size(),
        verify_hash=verify_hash,
    )

    # If we skip hash verification, fall back to the digest from the OCI manifest
    final_hash = calculated_hash if verify_hash else target_digest.replace("sha256:", "")

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
    download_file_native(
        url=blob_url,
        dest_path=dest_file,
        auth_token=oci_token,
        chunk_size=_resolve_chunk_size(),
        verify_hash=_resolve_verify_hash(),
    )
    return dest_file


def _create_symlink(src: str, dst: str):
    """Create a symlink with silent fallback for Windows or restricted filesystems."""
    if os.path.exists(dst):
        os.remove(dst)

    os.makedirs(os.path.dirname(dst), exist_ok=True)

    try:
        # Relative path from the snapshot directory to the blob
        rel_src = os.path.relpath(src, os.path.dirname(dst))
        os.symlink(rel_src, dst)
    except OSError:
        # Fallback 1: Hardlink
        try:
            os.link(src, dst)
        except OSError:
            # Fallback 2: Full plain copy (silent)
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
