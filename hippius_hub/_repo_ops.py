"""Module-level repo CRUD + inspection: create_repo, delete_repo, repo_info,
model_info, list_repo_files, repo_exists, file_exists, revision_exists.

Each function maps a HF API call onto Harbor's /api/v2.0 admin endpoints plus
the OCI v2 manifest API. Functions accept a `token` kwarg in HF's three-state
shape (None/True=saved, False=no auth, str=use literal).
"""
from typing import List, Optional, Union

import httpx
from huggingface_hub import ModelInfo, RepoUrl
from huggingface_hub.hf_api import RepoSibling

from ._harbor import (
    FORBIDDEN,
    harbor_create_project,
    harbor_delete_repository,
    harbor_get_artifact,
    harbor_get_project,
    harbor_get_repository,
    split_repo_id,
)
from ._oci import fetch_manifest, head_manifest, iter_titled_layers, layer_titles
from .auth import get_oci_bearer_token, get_token, resolve_auth_header, resolve_token_value
from .constants import DEFAULT_HTTP_TIMEOUT, resolve_registry
from .errors import (
    EntryNotFoundError,
    LocalTokenNotFoundError,
    RepositoryNotFoundError,
    RevisionNotFoundError,
)
from .file_download import _oci_repo_path, _validate_repo_type


def _build_repo_url(repo_id: str, endpoint: Optional[str]) -> RepoUrl:
    base = resolve_registry(endpoint)
    return RepoUrl(f"{base}/v2/{repo_id}", endpoint=base)


def _list_tags(registry: str, repo_id: str, oci_token: str) -> Optional[list]:
    """Return list of tags for a repo via the OCI v2 distribution API.

    Robots with `pull` perm can call this. Returns None on 404 (repo doesn't exist),
    a (possibly empty) list otherwise.
    """
    resp = httpx.get(
        f"{registry}/v2/{repo_id}/tags/list",
        headers={"Authorization": f"Bearer {oci_token}"},
        timeout=DEFAULT_HTTP_TIMEOUT,
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json().get("tags") or []


# ---- repo CRUD ----

def create_repo(
    repo_id: str,
    *,
    token: Union[bool, str, None] = None,
    private: Optional[bool] = None,
    visibility: Optional[str] = None,
    repo_type: Optional[str] = None,
    exist_ok: bool = False,
    endpoint: Optional[str] = None,
    **kwargs,
) -> RepoUrl:
    """Ensure the underlying Harbor project exists; return a RepoUrl.

    Hippius repositories materialize on first push, so for an existing project
    this is essentially a no-op that returns the URL. If the project doesn't
    exist yet, attempts to create it (requires project_create permission on
    the token / robot account).
    """
    _validate_repo_type(repo_type)
    oci_repo = _oci_repo_path(repo_id, repo_type)
    project, _ = split_repo_id(oci_repo)
    auth_header = resolve_auth_header(token)
    if auth_header is None:
        raise LocalTokenNotFoundError(
            "Authentication required; run `hippius-hub login` first."
        )

    # Use the OCI tags/list endpoint to detect existing repos — accessible to
    # any account with pull perms, unlike Harbor's admin project API.
    oci_token = get_oci_bearer_token(oci_repo, resolve_token_value(token), push=False)
    tags = _list_tags(resolve_registry(endpoint), oci_repo, oci_token)

    if tags is not None:
        # Repository already exists in the registry
        if tags and not exist_ok:
            from .errors import HfHubHTTPError
            from httpx import Response, Request
            fake_resp = Response(409, request=Request("GET", resolve_registry(endpoint)))
            raise HfHubHTTPError(
                f"Repository {repo_id!r} already exists and exist_ok=False",
                response=fake_resp,
            )
        return _build_repo_url(oci_repo, endpoint)

    # Repository doesn't exist; ensure the underlying Harbor project exists.
    existing_project = harbor_get_project(auth_header, project, endpoint=endpoint)
    if existing_project is None:
        public = (visibility == "public") if visibility else (not bool(private))
        harbor_create_project(auth_header, project, public=public, endpoint=endpoint)

    return _build_repo_url(oci_repo, endpoint)


def delete_repo(
    repo_id: str,
    *,
    token: Union[bool, str, None] = None,
    repo_type: Optional[str] = None,
    missing_ok: bool = False,
    endpoint: Optional[str] = None,
) -> None:
    """Delete the Harbor repository (project/repo)."""
    _validate_repo_type(repo_type)
    project, repo = split_repo_id(_oci_repo_path(repo_id, repo_type))
    auth_header = resolve_auth_header(token)
    if auth_header is None:
        raise RepositoryNotFoundError("delete_repo requires authentication")
    harbor_delete_repository(
        auth_header, project, repo, endpoint=endpoint, missing_ok=missing_ok,
    )


# ---- inspection ----

def repo_info(
    repo_id: str,
    *,
    revision: Optional[str] = None,
    repo_type: Optional[str] = None,
    timeout: Optional[float] = None,
    files_metadata: bool = False,
    token: Union[bool, str, None] = None,
    endpoint: Optional[str] = None,
    **kwargs,
) -> ModelInfo:
    """Return a ModelInfo for `repo_id` at `revision`.

    Combines Harbor's per-repository metadata (created_at, update_time, public
    flag inherited from the project) with the OCI manifest's layers
    (file list and per-file sizes). Fields HF reports but we can't derive
    from OCI (pipeline_tag, library_name, tags, downloads, likes) are left None.
    """
    _validate_repo_type(repo_type)
    if revision is None:
        revision = "main"
    oci_repo = _oci_repo_path(repo_id, repo_type)
    project, repo = split_repo_id(oci_repo)

    auth_header = resolve_auth_header(token)
    # Harbor lookups are best-effort: robot accounts often lack admin-API
    # perms (returns None). The OCI manifest fetch below is the source of truth.
    harbor_repo = harbor_get_repository(auth_header, project, repo, endpoint=endpoint) if auth_header else None
    harbor_project = harbor_get_project(auth_header, project, endpoint=endpoint) if auth_header else None

    oci_token = get_oci_bearer_token(oci_repo, resolve_token_value(token), push=False)
    # Read path: we only need the manifest body, not the digest — `repo_info`
    # never PUTs, so there's no If-Match to thread.
    manifest = fetch_manifest(resolve_registry(endpoint), oci_repo, revision, oci_token).manifest

    # ModelInfo's __init__ treats each entry in `siblings` as a dict and
    # builds the RepoSibling internally — pass raw dicts here.
    siblings = [
        {"rfilename": title, "size": layer.get("size"), "blobId": layer.get("digest")}
        for title, layer in iter_titled_layers(manifest)
    ]

    # Per-revision metadata: artifact info has push_time
    artifact = harbor_get_artifact(auth_header, project, repo, revision, endpoint=endpoint) if auth_header else None

    last_modified = None
    created_at = None
    # Distinguish "we read it and saw no value" from "we couldn't read it" so
    # we don't mis-report a public repo as private when the caller's token
    # lacks Harbor admin-API perms (e.g. robot accounts get 403 here).
    if isinstance(artifact, dict):
        last_modified = artifact.get("push_time") or artifact.get("update_time")
    if isinstance(harbor_repo, dict):
        last_modified = last_modified or harbor_repo.get("update_time")
        created_at = harbor_repo.get("creation_time")
    if isinstance(harbor_project, dict):
        private = not bool(harbor_project.get("metadata", {}).get("public", False))
    else:
        # FORBIDDEN or None: leave as unknown rather than assuming private.
        private = None

    return ModelInfo(
        id=repo_id,
        sha=manifest.get("config", {}).get("digest"),
        lastModified=last_modified,
        createdAt=created_at,
        private=private,
        gated=False,
        disabled=False,
        siblings=siblings,
        tags=[],
        pipeline_tag=None,
        library_name=None,
        downloads=None,
        likes=None,
    )


def model_info(
    repo_id: str,
    *,
    revision: Optional[str] = None,
    token: Union[bool, str, None] = None,
    timeout: Optional[float] = None,
    files_metadata: bool = False,
    endpoint: Optional[str] = None,
    **kwargs,
) -> ModelInfo:
    return repo_info(
        repo_id,
        revision=revision,
        repo_type="model",
        timeout=timeout,
        files_metadata=files_metadata,
        token=token,
        endpoint=endpoint,
        **kwargs,
    )


def list_repo_files(
    repo_id: str,
    *,
    revision: Optional[str] = None,
    repo_type: Optional[str] = None,
    token: Union[bool, str, None] = None,
    endpoint: Optional[str] = None,
) -> List[str]:
    _validate_repo_type(repo_type)
    if revision is None:
        revision = "main"

    oci_repo = _oci_repo_path(repo_id, repo_type)
    oci_token = get_oci_bearer_token(oci_repo, resolve_token_value(token), push=False)
    # Read path: digest isn't needed because we don't PUT here.
    manifest = fetch_manifest(resolve_registry(endpoint), oci_repo, revision, oci_token).manifest
    return layer_titles(manifest)


def repo_exists(
    repo_id: str,
    *,
    repo_type: Optional[str] = None,
    token: Union[bool, str, None] = None,
    endpoint: Optional[str] = None,
) -> bool:
    """True iff the OCI repository has ever been pushed to (any tag exists)."""
    _validate_repo_type(repo_type)
    oci_repo = _oci_repo_path(repo_id, repo_type)
    oci_token = get_oci_bearer_token(oci_repo, resolve_token_value(token), push=False)
    tags = _list_tags(resolve_registry(endpoint), oci_repo, oci_token)
    return tags is not None and len(tags) > 0


def revision_exists(
    repo_id: str,
    revision: str,
    *,
    repo_type: Optional[str] = None,
    token: Union[bool, str, None] = None,
    endpoint: Optional[str] = None,
) -> bool:
    _validate_repo_type(repo_type)
    oci_repo = _oci_repo_path(repo_id, repo_type)
    oci_token = get_oci_bearer_token(oci_repo, resolve_token_value(token), push=False)
    head = head_manifest(resolve_registry(endpoint), oci_repo, revision, oci_token)
    return head.status_code == 200


def file_exists(
    repo_id: str,
    filename: str,
    *,
    revision: Optional[str] = None,
    repo_type: Optional[str] = None,
    token: Union[bool, str, None] = None,
    endpoint: Optional[str] = None,
) -> bool:
    _validate_repo_type(repo_type)
    if revision is None:
        revision = "main"
    oci_repo = _oci_repo_path(repo_id, repo_type)
    oci_token = get_oci_bearer_token(oci_repo, resolve_token_value(token), push=False)
    # Read path: digest isn't needed because we don't PUT here. `missing_ok`
    # lets us return False for a fresh repo without raising.
    result = fetch_manifest(resolve_registry(endpoint), oci_repo, revision, oci_token, missing_ok=True)
    if result is None:
        return False
    return filename in layer_titles(result.manifest)
