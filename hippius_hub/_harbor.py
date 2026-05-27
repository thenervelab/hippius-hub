"""Thin wrappers over the Harbor /api/v2.0/ admin API.

Kept separate from auth.py to isolate HTTP plumbing from token management.
Each helper takes the caller's auth header verbatim so the caller controls
credential resolution (saved token, env var, fresh login, etc.).
"""
import base64
from typing import Optional

import httpx

from .constants import DEFAULT_HTTP_TIMEOUT, resolve_registry
from .errors import LocalTokenNotFoundError


def _base(endpoint: Optional[str]) -> str:
    return resolve_registry(endpoint)


def _headers(auth_header: str, *, json: bool = False) -> dict:
    h = {"Authorization": auth_header}
    if json:
        h["Content-Type"] = "application/json"
    return h


def _decode_basic_username(auth_header: str) -> Optional[str]:
    """Return the username from a `Basic <b64>` header, or None for non-Basic."""
    prefix = "Basic "
    if not auth_header.startswith(prefix):
        return None
    decoded = base64.b64decode(auth_header[len(prefix):]).decode("utf-8", errors="replace")
    return decoded.split(":", 1)[0]


def harbor_whoami(auth_header: str, endpoint: Optional[str] = None) -> dict:
    """Return user info shaped like huggingface_hub.whoami's response.

    For robot accounts (Harbor service identities), Harbor's /users/current
    returns 401 because robots aren't users. Detect via the Basic auth username
    prefix and synthesize a dict — robots have no email/fullname to report.
    """
    username = _decode_basic_username(auth_header)
    if username is not None and username.startswith("robot$"):
        return {
            "type": "robot",
            "id": username,
            "name": username,
            "fullname": None,
            "email": None,
            "emailVerified": False,
            "isPro": False,
            "orgs": [],
        }

    resp = httpx.get(
        f"{_base(endpoint)}/api/v2.0/users/current",
        headers=_headers(auth_header),
    )
    if resp.status_code == 401:
        raise LocalTokenNotFoundError(
            "Saved credentials were rejected by the registry; "
            "run `hippius-hub login` again."
        )
    resp.raise_for_status()
    h = resp.json()
    return {
        "type": "user",
        "id": str(h.get("user_id", "")),
        "name": h["username"],
        "fullname": h.get("realname") or h["username"],
        "email": h.get("email") or "",
        "emailVerified": True,
        "isPro": False,
        "orgs": [],
    }


def harbor_create_project(
    auth_header: str,
    project_name: str,
    *,
    public: bool = False,
    endpoint: Optional[str] = None,
) -> int:
    """Create a Harbor project. Returns the new project id.

    Raises httpx.HTTPStatusError on failure. 409 means the project already exists.
    """
    resp = httpx.post(
        f"{_base(endpoint)}/api/v2.0/projects",
        headers=_headers(auth_header, json=True),
        json={"project_name": project_name, "public": public},
    )
    resp.raise_for_status()
    location = resp.headers.get("Location", "")
    if location and location.startswith("/api/v2.0/projects/"):
        return int(location.rsplit("/", 1)[-1])
    return -1


FORBIDDEN = object()  # sentinel distinguishing "401/403, can't see" from "404, doesn't exist"


def harbor_get_project(
    auth_header: str,
    project_name: str,
    *,
    endpoint: Optional[str] = None,
):
    """Return Harbor project info dict, None if 404, or FORBIDDEN if 401/403.

    Callers that only care about existence vs not-existence can check
    `result is None`; callers building user-facing metadata (e.g. `private`
    flag on ModelInfo) should treat FORBIDDEN as "unknown" rather than
    inferring private/public from absent data.
    """
    resp = httpx.get(
        f"{_base(endpoint)}/api/v2.0/projects/{project_name}",
        headers=_headers(auth_header),
        timeout=DEFAULT_HTTP_TIMEOUT,
    )
    if resp.status_code in (401, 403):
        return FORBIDDEN
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def harbor_delete_project(
    auth_header: str,
    project_name: str,
    *,
    endpoint: Optional[str] = None,
    missing_ok: bool = False,
) -> None:
    """Delete a Harbor project; with `missing_ok=True`, 404 is swallowed."""
    resp = httpx.delete(
        f"{_base(endpoint)}/api/v2.0/projects/{project_name}",
        headers=_headers(auth_header),
    )
    if resp.status_code == 404 and missing_ok:
        return
    resp.raise_for_status()


def harbor_get_repository(
    auth_header: str,
    project_name: str,
    repo_name: str,
    *,
    endpoint: Optional[str] = None,
) -> Optional[dict]:
    """Return Harbor repository metadata, or None if it doesn't exist.

    Harbor repository names can contain `/` (e.g. project=test, repo=foo/bar).
    Harbor encodes those as `%2F` in the URL.
    """
    encoded = repo_name.replace("/", "%2F")
    resp = httpx.get(
        f"{_base(endpoint)}/api/v2.0/projects/{project_name}/repositories/{encoded}",
        headers=_headers(auth_header),
        timeout=DEFAULT_HTTP_TIMEOUT,
    )
    if resp.status_code in (401, 403):
        return FORBIDDEN
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def harbor_delete_repository(
    auth_header: str,
    project_name: str,
    repo_name: str,
    *,
    endpoint: Optional[str] = None,
    missing_ok: bool = False,
) -> None:
    """Delete a Harbor repository under `project_name`; with `missing_ok=True`, 404 is swallowed."""
    encoded = repo_name.replace("/", "%2F")
    resp = httpx.delete(
        f"{_base(endpoint)}/api/v2.0/projects/{project_name}/repositories/{encoded}",
        headers=_headers(auth_header),
    )
    if resp.status_code == 404 and missing_ok:
        return
    resp.raise_for_status()


def harbor_get_artifact(
    auth_header: str,
    project_name: str,
    repo_name: str,
    reference: str,
    *,
    endpoint: Optional[str] = None,
    with_label: bool = False,
    with_tag: bool = True,
) -> Optional[dict]:
    """Return Harbor artifact info (covers per-revision metadata: push_time, etc.)."""
    encoded = repo_name.replace("/", "%2F")
    params = {
        "with_label": str(with_label).lower(),
        "with_tag": str(with_tag).lower(),
    }
    resp = httpx.get(
        f"{_base(endpoint)}/api/v2.0/projects/{project_name}/repositories/{encoded}/artifacts/{reference}",
        headers=_headers(auth_header),
        params=params,
        timeout=DEFAULT_HTTP_TIMEOUT,
    )
    if resp.status_code in (401, 403):
        return FORBIDDEN
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def split_repo_id(repo_id: str) -> tuple:
    """Split `repo_id` into (project, repository) by the first `/`.

    Single-segment repo_id (no `/`) is invalid — Harbor requires a project.
    Multi-slash repo_id (e.g. `proj/foo/bar`) keeps everything after the first
    `/` as the repository name.
    """
    if "/" not in repo_id:
        raise ValueError(
            f"repo_id {repo_id!r} must be in 'project/repository' form for the Hippius registry"
        )
    project, repo = repo_id.split("/", 1)
    return project, repo
