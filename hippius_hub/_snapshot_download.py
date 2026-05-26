import os
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Union

from huggingface_hub.utils import filter_repo_objects

from ._oci import fetch_manifest, layer_titles
from .auth import get_oci_bearer_token, resolve_token_value
from .constants import DEFAULT_CACHE_DIR, resolve_registry
from .errors import LocalEntryNotFoundError
from .file_download import _cache_dirname, _oci_repo_path, _validate_repo_type, hf_hub_download


def _handle_ignored_snapshot_kwargs(
    *,
    etag_timeout: float,
    tqdm_class,
    headers,
    user_agent,
    library_name,
    library_version,
):
    """Emit UserWarning for HF kwargs snapshot_download accepts but ignores.

    Excludes dry_run — snapshot_download honors it. stacklevel=3 points at
    the user's call site, not this helper.
    """
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


def snapshot_download(
    repo_id: str,
    *,
    repo_type: Optional[str] = None,
    revision: Optional[str] = None,
    cache_dir: Union[str, Path, None] = None,
    local_dir: Union[str, Path, None] = None,
    library_name: Optional[str] = None,
    library_version: Optional[str] = None,
    user_agent: Union[Dict, str, None] = None,
    etag_timeout: float = 10.0,
    force_download: bool = False,
    token: Union[bool, str, None] = None,
    local_files_only: bool = False,
    allow_patterns: Optional[Union[List[str], str]] = None,
    ignore_patterns: Optional[Union[List[str], str]] = None,
    max_workers: int = 8,
    tqdm_class: Optional[type] = None,
    headers: Optional[Dict[str, str]] = None,
    endpoint: Optional[str] = None,
    dry_run: bool = False,
) -> str:
    """Download every file in an OCI manifest for `repo_id` at `revision`.

    Returns the path to the snapshot directory (or `local_dir` if provided).
    Honors allow_patterns/ignore_patterns via huggingface_hub.utils.filter_repo_objects,
    plus dry_run (early-returns without downloading).
    Accepted but currently ignored (UserWarning at call site):
        etag_timeout (when != 10.0), tqdm_class, headers, user_agent,
        library_name, library_version.
    """
    _validate_repo_type(repo_type)
    _handle_ignored_snapshot_kwargs(
        etag_timeout=etag_timeout,
        tqdm_class=tqdm_class,
        headers=headers,
        user_agent=user_agent,
        library_name=library_name,
        library_version=library_version,
    )
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR
    cache_dir = str(cache_dir)
    if revision is None:
        revision = "main"

    if local_dir is not None:
        snapshot_dir = str(local_dir)
    else:
        snapshot_dir = os.path.join(
            cache_dir,
            _cache_dirname(repo_id, repo_type),
            "snapshots",
            revision,
        )

    if local_files_only:
        if not os.path.exists(snapshot_dir):
            raise LocalEntryNotFoundError(
                f"Snapshot for '{repo_id}' at revision '{revision}' "
                f"not found in local cache (cache_dir={cache_dir!r})"
            )
        return snapshot_dir

    oci_repo = _oci_repo_path(repo_id, repo_type)
    registry = resolve_registry(endpoint)
    auth_token = resolve_token_value(token)
    oci_token = get_oci_bearer_token(oci_repo, auth_token)

    # Read path: snapshot_download never PUTs, so we discard the digest and
    # pass the bare manifest body through to each worker via _resolved_manifest
    # (which is typed as a dict — see `hf_hub_download`).
    manifest = fetch_manifest(registry, oci_repo, revision, oci_token).manifest
    filenames = layer_titles(manifest)

    filtered = list(
        filter_repo_objects(
            items=filenames,
            allow_patterns=allow_patterns,
            ignore_patterns=ignore_patterns,
        )
    )

    if dry_run:
        return snapshot_dir

    def _download_one(filename: str) -> str:
        # Pass through the already-fetched manifest + OCI token so each worker
        # avoids redoing the manifest GET and token-service round-trip — that
        # was an N+1 latency cliff for large snapshots.
        return hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            repo_type=repo_type,
            revision=revision,
            cache_dir=cache_dir,
            local_dir=local_dir,
            force_download=force_download,
            token=token,
            endpoint=endpoint,
            _resolved_manifest=manifest,
            _oci_token=oci_token,
        )

    if filtered:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_download_one, name) for name in filtered]
            for fut in as_completed(futures):
                fut.result()

    return snapshot_dir
