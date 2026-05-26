"""Low-level OCI distribution helpers shared across hippius_hub modules.

Centralizes manifest fetch, layer iteration, and the OCI v2 accept header
so the same plumbing isn't reimplemented in each module that touches the
registry.
"""
from dataclasses import dataclass
from typing import Iterator, List, Optional, Tuple

import httpx

from .constants import DEFAULT_HTTP_TIMEOUT, LAYER_TITLE_KEY, OCI_MANIFEST_ACCEPT
from .errors import RevisionNotFoundError


@dataclass(frozen=True)
class ManifestResult:
    """Result of fetching an OCI manifest, with the digest needed for If-Match.

    `digest` is the value of the `Docker-Content-Digest` response header
    (e.g. `sha256:abc...`). It's required to send `If-Match` on the next
    PUT so the server rejects (412) any concurrent writer that has already
    advanced the revision past what we just read. `digest` is `None` only
    when the server didn't return that header (some registries omit it on
    older manifest media types) — the caller should then either skip the
    If-Match check or fail closed depending on policy.
    """

    manifest: dict
    digest: Optional[str]


def oci_headers(oci_token: str) -> dict:
    """Build the OCI v2 Authorization + manifest Accept headers."""
    return {"Authorization": f"Bearer {oci_token}", "Accept": OCI_MANIFEST_ACCEPT}


def manifest_url(registry: str, repo_id: str, revision: str) -> str:
    """Build the OCI v2 manifest URL for `repo_id` at `revision`."""
    return f"{registry}/v2/{repo_id}/manifests/{revision}"


def fetch_manifest(
    registry: str,
    repo_id: str,
    revision: str,
    oci_token: str,
    *,
    missing_ok: bool = False,
) -> Optional[ManifestResult]:
    """GET the OCI manifest for repo_id:revision.

    On 404: returns None if `missing_ok`, else raises RevisionNotFoundError
    with the response attached. Other non-2xx statuses propagate via
    `raise_for_status()`.

    The returned `ManifestResult` carries both the decoded manifest dict and
    the `Docker-Content-Digest` response header — callers that intend to PUT
    a new manifest at the same revision should thread `result.digest` into
    the PUT as `If-Match` to get optimistic-concurrency rejection (412) when
    a racing writer has advanced the revision.
    """
    resp = httpx.get(
        manifest_url(registry, repo_id, revision),
        headers=oci_headers(oci_token),
        timeout=DEFAULT_HTTP_TIMEOUT,
    )
    if resp.status_code == 404:
        if missing_ok:
            return None
        raise RevisionNotFoundError(
            f"Revision {revision!r} not found in repository {repo_id!r}",
            response=resp,
        )
    resp.raise_for_status()
    return ManifestResult(manifest=resp.json(), digest=resp.headers.get("Docker-Content-Digest"))


def head_manifest(
    registry: str,
    repo_id: str,
    revision: str,
    oci_token: str,
) -> httpx.Response:
    """HEAD the OCI manifest for `repo_id:revision` (used for cheap existence checks)."""
    return httpx.head(
        manifest_url(registry, repo_id, revision),
        headers=oci_headers(oci_token),
        timeout=DEFAULT_HTTP_TIMEOUT,
    )


def layer_title(layer: dict) -> Optional[str]:
    """Return the in-repo filename annotation on a layer, or None if absent."""
    return layer.get("annotations", {}).get(LAYER_TITLE_KEY)


def iter_titled_layers(manifest: dict) -> Iterator[Tuple[str, dict]]:
    """Yield (title, layer) for each manifest layer with a title annotation.
    Layers without a title are silently skipped."""
    for layer in manifest.get("layers", []):
        title = layer_title(layer)
        if title:
            yield title, layer


def layer_titles(manifest: dict) -> List[str]:
    """Return the list of in-repo filenames present in a manifest."""
    return [title for title, _ in iter_titled_layers(manifest)]
