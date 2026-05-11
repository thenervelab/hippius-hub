import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Union

import httpx
from huggingface_hub.utils import filter_repo_objects

from .auth import get_oci_bearer_token
from .constants import DEFAULT_CACHE_DIR, DEFAULT_REGISTRY_URL
from .errors import LocalEntryNotFoundError, RevisionNotFoundError
from .file_download import _resolve_auth_token, _validate_repo_type, hf_hub_download


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
    Honors allow_patterns/ignore_patterns via huggingface_hub.utils.filter_repo_objects.
    library_name/library_version/user_agent/etag_timeout/tqdm_class/headers are
    accepted for HF signature parity and currently have no effect.
    """
    _validate_repo_type(repo_type)
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
            f"models--{repo_id.replace('/', '--')}",
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

    registry = (endpoint or DEFAULT_REGISTRY_URL).rstrip("/")
    auth_token = _resolve_auth_token(token)
    oci_token = get_oci_bearer_token(repo_id, auth_token)

    manifest_url = f"{registry}/v2/{repo_id}/manifests/{revision}"
    req_headers = {
        "Authorization": f"Bearer {oci_token}",
        "Accept": "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json",
    }
    resp = httpx.get(manifest_url, headers=req_headers)
    if resp.status_code == 404:
        raise RevisionNotFoundError(
            f"Revision '{revision}' not found in repository '{repo_id}'",
            response=resp,
        )
    resp.raise_for_status()

    manifest = resp.json()
    filenames = [
        layer.get("annotations", {}).get("org.opencontainers.image.title")
        for layer in manifest.get("layers", [])
    ]
    filenames = [f for f in filenames if f]

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
