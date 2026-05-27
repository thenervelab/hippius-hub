"""Module-level repo CRUD + inspection: create_repo, delete_repo, repo_info,
model_info, list_repo_files, repo_exists, file_exists, revision_exists.

Each function maps a HF API call onto Harbor's /api/v2.0 admin endpoints plus
the OCI v2 manifest API. Functions accept a `token` kwarg in HF's three-state
shape (None/True=saved, False=no auth, str=use literal).
"""
from typing import List, Optional, Union

import httpx
from huggingface_hub import GitRefInfo, GitRefs, ModelInfo, RepoUrl
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
    # Follow RFC 5988 `Link: rel="next"` pagination so repos with many tags
    # return the complete list, not just the registry's first page — callers
    # like list_repo_refs / `revisions` need every tag, not a prefix.
    headers = {"Authorization": f"Bearer {oci_token}"}
    url = f"{registry}/v2/{repo_id}/tags/list"
    tags: list = []
    seen = set()
    while url and url not in seen:
        seen.add(url)
        resp = httpx.get(url, headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        tags.extend(resp.json().get("tags") or [])
        url = _next_link(resp.headers.get("Link"), registry)
    return tags


def _next_link(link_header: Optional[str], registry: str) -> Optional[str]:
    """Extract the absolute `rel="next"` URL from an RFC 5988 Link header, or
    None when there's no next page. Distribution registries return the next
    page as a root-relative path (e.g. `</v2/repo/tags/list?last=x&n=y>`)."""
    if not link_header:
        return None
    for part in link_header.split(","):
        segments = part.split(";")
        if len(segments) < 2:
            continue
        rel = "".join(segments[1:]).replace(" ", "").replace('"', "").lower()
        if "rel=next" not in rel:
            continue
        target = segments[0].strip().lstrip("<").rstrip(">").strip()
        if target.startswith("http://") or target.startswith("https://"):
            return target
        return f"{registry}{target}"
    return None


def _manifest_digest(registry: str, repo_id: str, revision: str, oci_token: str) -> Optional[str]:
    """Return the content digest of a revision's manifest, or None if unreachable.

    Used as the commit-like identifier for a revision. A cheap HEAD suffices —
    the digest comes back in the Docker-Content-Digest response header.
    """
    head = head_manifest(registry, repo_id, revision, oci_token)
    if head.status_code != 200:
        return None
    return head.headers.get("Docker-Content-Digest")


def _revision_created(registry: str, repo_id: str, revision: str, oci_token: str) -> Optional[str]:
    """Return a revision's upload timestamp (ISO8601), or None if absent.

    Read from the manifest's org.opencontainers.image.created annotation, which
    uploads via this tool stamp. Revisions pushed by other tooling may lack it.
    """
    manifest = fetch_manifest(registry, repo_id, revision, oci_token, missing_ok=True)
    if manifest is None:
        return None
    return manifest.get("annotations", {}).get("org.opencontainers.image.created")


def _normalize_oci_timestamp(ts: Optional[str]) -> Optional[str]:
    """Convert an OCI `created` annotation to the shape huggingface_hub accepts.

    Uploads stamp it with `datetime.isoformat()`, which yields an offset form
    like `2026-05-26T18:05:32.733878+00:00`. ModelInfo runs HF's parse_datetime,
    which only accepts the `...Z` form and raises ValueError on the offset — so
    we reshape it here. Returns None when absent or unparseable: a bad timestamp
    should drop the field, not crash repo_info.
    """
    if not ts:
        return None
    from datetime import datetime, timezone
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


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
        # Same HfHubHTTPError-needs-a-response requirement as above; without
        # this the raise itself throws TypeError instead of the intended error.
        from httpx import Request, Response
        fake_resp = Response(401, request=Request("GET", resolve_registry(endpoint)))
        raise RepositoryNotFoundError("delete_repo requires authentication", response=fake_resp)
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
    manifest = fetch_manifest(resolve_registry(endpoint), oci_repo, revision, oci_token)

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
    # Fallback to the manifest's upload timestamp when Harbor admin data is
    # unavailable (e.g. pull-only robot accounts get FORBIDDEN above).
    # Normalized to HF's expected `...Z` form — the raw annotation is an offset
    # ISO string that ModelInfo's parse_datetime would reject.
    manifest_created = _normalize_oci_timestamp(
        manifest.get("annotations", {}).get("org.opencontainers.image.created")
    )
    last_modified = last_modified or manifest_created
    created_at = created_at or manifest_created
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
    manifest = fetch_manifest(resolve_registry(endpoint), oci_repo, revision, oci_token)
    return layer_titles(manifest)


def list_repo_refs(
    repo_id: str,
    *,
    repo_type: Optional[str] = None,
    include_pull_requests: bool = False,
    token: Union[bool, str, None] = None,
    endpoint: Optional[str] = None,
) -> GitRefs:
    """List a repository's revisions as a HF-compatible GitRefs.

    Each revision (an OCI manifest tag) maps to a GitRefInfo. The revision named
    `main` — the mutable default that uploads re-point — is reported under
    `branches`; every other revision behaves like a release and is reported under
    `tags`.

    `target_commit` is left None: the registry's tag list doesn't carry the
    manifest digest, and resolving it would be one HEAD per revision (O(N)
    round-trips — minutes on repos with many revisions). Callers that need a
    specific revision's digest can read `repo_info(repo_id, revision=...).sha`.

    `include_pull_requests` is accepted for HF signature parity but has no effect:
    the Hippius registry has no pull-request refs.
    """
    _validate_repo_type(repo_type)
    oci_repo = _oci_repo_path(repo_id, repo_type)
    registry = resolve_registry(endpoint)
    oci_token = get_oci_bearer_token(oci_repo, resolve_token_value(token), push=False)
    tags = _list_tags(registry, oci_repo, oci_token)
    if tags is None:
        # RepositoryNotFoundError (an HfHubHTTPError) requires a response object;
        # synthesize a 404 like create_repo does above.
        from httpx import Request, Response
        fake_resp = Response(404, request=Request("GET", registry))
        raise RepositoryNotFoundError(f"Repository {repo_id!r} not found", response=fake_resp)

    branches = []
    tag_refs = []
    for tag in tags:
        if tag == "main":
            branches.append(GitRefInfo(name=tag, ref="refs/heads/main", target_commit=None))
        else:
            tag_refs.append(GitRefInfo(name=tag, ref=f"refs/tags/{tag}", target_commit=None))

    return GitRefs(branches=branches, converts=[], tags=tag_refs)


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
    manifest = fetch_manifest(resolve_registry(endpoint), oci_repo, revision, oci_token, missing_ok=True)
    if manifest is None:
        return False
    return filename in layer_titles(manifest)
