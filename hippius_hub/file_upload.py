import datetime
import hashlib
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import BinaryIO, Dict, List, Optional, Union

import httpx
from huggingface_hub import CommitInfo
from huggingface_hub.utils import filter_repo_objects
from tqdm import tqdm

from .auth import get_oci_bearer_token, get_token
from .constants import DEFAULT_REGISTRY_URL
from .file_download import _resolve_auth_token, _validate_repo_type

try:
    from .hippius_core import hash_file_native, upload_blob_native
except ImportError:
    raise ImportError("hippius_core is not installed. Did you run `maturin develop`?")


# ---- helpers ----

def _registry(endpoint: Optional[str]) -> str:
    return (endpoint or DEFAULT_REGISTRY_URL).rstrip("/")


def _oci_bearer(repo_id: str, token, push: bool = True) -> str:
    return get_oci_bearer_token(repo_id, _resolve_auth_token(token), push=push)


def _accept_header() -> str:
    return ("application/vnd.oci.image.manifest.v1+json, "
            "application/vnd.docker.distribution.manifest.v2+json")


def _fetch_existing_manifest(registry: str, repo_id: str, revision: str, oci_token: str) -> Optional[dict]:
    """Return current manifest dict for repo_id:revision, or None if it doesn't exist."""
    url = f"{registry}/v2/{repo_id}/manifests/{revision}"
    headers = {"Authorization": f"Bearer {oci_token}", "Accept": _accept_header()}
    resp = httpx.get(url, headers=headers, timeout=30.0)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def _empty_config_blob_descriptor() -> tuple:
    data = b"{}"
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
    return data, digest, len(data)


def _ensure_blob_uploaded(
    registry: str,
    repo_id: str,
    oci_token: str,
    file_path: str,
    sha256_hash: str,
    file_size: int,
) -> None:
    """POST/PUT a blob if not already present at its digest."""
    digest = f"sha256:{sha256_hash}"
    headers = {"Authorization": f"Bearer {oci_token}"}
    check = httpx.head(f"{registry}/v2/{repo_id}/blobs/{digest}", headers=headers, timeout=30.0)
    if check.status_code == 200:
        return

    init_headers = {**headers, "Content-Length": "0"}
    init = httpx.post(f"{registry}/v2/{repo_id}/blobs/uploads/", headers=init_headers, timeout=30.0)
    init.raise_for_status()
    location = init.headers.get("Location")
    if not location:
        raise ValueError("Registry did not return a Location header for upload initiation")
    if location.startswith("/"):
        location = f"{registry}{location}"
    sep = "&" if "?" in location else "?"
    upload_blob_native(f"{location}{sep}digest={digest}", file_path, oci_token)


def _ensure_config_blob_uploaded(registry: str, repo_id: str, oci_token: str) -> tuple:
    """Push the empty-object config blob if missing. Returns (digest, size)."""
    data, digest, size = _empty_config_blob_descriptor()
    headers = {"Authorization": f"Bearer {oci_token}"}
    check = httpx.head(f"{registry}/v2/{repo_id}/blobs/{digest}", headers=headers, timeout=30.0)
    if check.status_code != 200:
        init = httpx.post(
            f"{registry}/v2/{repo_id}/blobs/uploads/",
            headers={**headers, "Content-Length": "0"},
            timeout=30.0,
        )
        init.raise_for_status()
        loc = init.headers.get("Location")
        if loc and loc.startswith("/"):
            loc = f"{registry}{loc}"
        sep = "&" if "?" in loc else "?"
        httpx.put(
            f"{loc}{sep}digest={digest}",
            headers={**headers, "Content-Type": "application/octet-stream"},
            content=data,
            timeout=30.0,
        )
    return digest, size


def _put_manifest(
    registry: str,
    repo_id: str,
    revision: str,
    oci_token: str,
    manifest: dict,
) -> dict:
    """PUT the manifest to revision. Returns the response (with digest in headers)."""
    url = f"{registry}/v2/{repo_id}/manifests/{revision}"
    headers = {
        "Authorization": f"Bearer {oci_token}",
        "Content-Type": "application/vnd.oci.image.manifest.v1+json",
    }
    resp = httpx.put(url, headers=headers, json=manifest, timeout=60.0)
    resp.raise_for_status()
    return resp


def _normalize_path_or_fileobj(path_or_fileobj) -> tuple:
    """Coerce HF's path_or_fileobj (str/Path/bytes/BinaryIO) into a (filesystem_path, cleanup).
    cleanup() must be called after use; it's a no-op for real paths."""
    if isinstance(path_or_fileobj, (str, Path)):
        return str(path_or_fileobj), lambda: None

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
    if isinstance(path_or_fileobj, bytes):
        tmp.write(path_or_fileobj)
    elif hasattr(path_or_fileobj, "read"):
        data = path_or_fileobj.read()
        if isinstance(data, str):
            data = data.encode("utf-8")
        tmp.write(data)
    else:
        tmp.close()
        os.unlink(tmp.name)
        raise TypeError(
            f"path_or_fileobj must be str/Path/bytes/BinaryIO, got {type(path_or_fileobj).__name__}"
        )
    tmp.flush()
    tmp.close()
    tmp_path = tmp.name
    return tmp_path, lambda: os.path.exists(tmp_path) and os.unlink(tmp_path)


def _build_layer(sha256_hash: str, file_size: int, path_in_repo: str) -> dict:
    return {
        "mediaType": "application/octet-stream",
        "size": file_size,
        "digest": f"sha256:{sha256_hash}",
        "annotations": {
            "org.opencontainers.image.title": path_in_repo.replace("\\", "/"),
        },
    }


def _merge_layers(
    existing: List[dict],
    new_layers: List[dict],
    delete_titles: Optional[set] = None,
) -> List[dict]:
    """Build a layer list combining `existing` with `new_layers`.
    New layers replace existing ones with the same title; titles in `delete_titles` are dropped."""
    delete_titles = delete_titles or set()
    by_title = {}
    for layer in existing:
        title = layer.get("annotations", {}).get("org.opencontainers.image.title")
        if title and title not in delete_titles:
            by_title[title] = layer
    for layer in new_layers:
        title = layer["annotations"]["org.opencontainers.image.title"]
        by_title[title] = layer
    return list(by_title.values())


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


def _warn_unsupported_kwargs(create_pr, parent_commit, run_as_future):
    if create_pr:
        raise NotImplementedError("create_pr=True is HF-specific; Hippius has no PR concept")
    if parent_commit:
        raise NotImplementedError("parent_commit is HF-specific; Hippius revisions are OCI tags")
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
    """
    _validate_repo_type(repo_type)
    _warn_unsupported_kwargs(create_pr, parent_commit, run_as_future)
    if revision is None:
        revision = "main"
    if commit_message is None:
        commit_message = f"Upload {path_in_repo}"
    if commit_description is None:
        commit_description = ""

    registry = _registry(endpoint)
    oci_token = _oci_bearer(repo_id, token, push=True)

    file_path, cleanup = _normalize_path_or_fileobj(path_or_fileobj)
    try:
        sha256_hash, file_size = hash_file_native(file_path)
        _ensure_blob_uploaded(registry, repo_id, oci_token, file_path, sha256_hash, file_size)
        new_layer = _build_layer(sha256_hash, file_size, path_in_repo)
    finally:
        cleanup()

    existing = _fetch_existing_manifest(registry, repo_id, revision, oci_token)
    existing_layers = existing.get("layers", []) if existing else []
    merged_layers = _merge_layers(existing_layers, [new_layer])

    config_digest, config_size = _ensure_config_blob_uploaded(registry, repo_id, oci_token)
    manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": config_digest,
            "size": config_size,
        },
        "layers": merged_layers,
        "annotations": _commit_annotations(commit_message, commit_description),
    }

    resp = _put_manifest(registry, repo_id, revision, oci_token, manifest)
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
) -> CommitInfo:
    """Upload every file under `folder_path` to a repository revision.

    Honors HF allow_patterns/ignore_patterns/delete_patterns. Merges with the
    existing manifest — any layer with a matching title is replaced; titles
    matching delete_patterns are removed from the new manifest entirely.
    """
    _validate_repo_type(repo_type)
    _warn_unsupported_kwargs(create_pr, parent_commit, run_as_future)
    if revision is None:
        revision = "main"
    if commit_message is None:
        commit_message = f"Upload folder using hippius_hub"
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

    registry = _registry(endpoint)
    oci_token = _oci_bearer(repo_id, token, push=True)

    def _process(rel_path: str) -> dict:
        abs_path = os.path.join(base_dir, rel_path)
        sha256_hash, file_size = hash_file_native(abs_path)
        digest = f"sha256:{sha256_hash}"
        headers = {"Authorization": f"Bearer {oci_token}"}
        check = httpx.head(f"{registry}/v2/{repo_id}/blobs/{digest}", headers=headers, timeout=30.0)
        repo_title = f"{path_in_repo}/{rel_path}" if path_in_repo else rel_path
        if check.status_code == 200:
            tqdm.write(f"✅ Already published (skipped): {repo_title}")
        else:
            tqdm.write(f"🚀 Uploading: {repo_title} ({file_size} bytes)...")
            _ensure_blob_uploaded(registry, repo_id, oci_token, abs_path, sha256_hash, file_size)
            tqdm.write(f"✅ Uploaded: {repo_title}")
        return _build_layer(sha256_hash, file_size, repo_title)

    new_layers = []
    if filtered:
        print(f"📦 Preparing to upload {len(filtered)} file(s) to {repo_id}:{revision}...")
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(_process, rel) for rel in filtered]
            for fut in tqdm(as_completed(futures), total=len(filtered), desc="Uploading", unit="file"):
                new_layers.append(fut.result())

    # Compute titles to delete from the existing manifest
    delete_titles = set()
    if delete_patterns:
        existing_preview = _fetch_existing_manifest(registry, repo_id, revision, oci_token)
        existing_titles = [
            layer.get("annotations", {}).get("org.opencontainers.image.title")
            for layer in (existing_preview or {}).get("layers", [])
        ]
        existing_titles = [t for t in existing_titles if t]
        delete_titles = set(filter_repo_objects(items=existing_titles, allow_patterns=delete_patterns))

    existing = _fetch_existing_manifest(registry, repo_id, revision, oci_token)
    existing_layers = existing.get("layers", []) if existing else []
    merged_layers = _merge_layers(existing_layers, new_layers, delete_titles=delete_titles)

    config_digest, config_size = _ensure_config_blob_uploaded(registry, repo_id, oci_token)
    manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": config_digest,
            "size": config_size,
        },
        "layers": merged_layers,
        "annotations": _commit_annotations(commit_message, commit_description),
    }

    print(f"📝 Publishing OCI Manifest for {revision}...")
    resp = _put_manifest(registry, repo_id, revision, oci_token, manifest)
    print(f"🎉 Successfully pushed {len(new_layers)} file(s) to {repo_id}:{revision}")
    return _build_commit_info(registry, repo_id, revision, resp, commit_message, commit_description)


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
