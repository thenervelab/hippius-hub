"""Low-level OCI distribution helpers shared across hippius_hub modules.

Centralizes manifest fetch, layer iteration, and the OCI v2 accept header
so the same plumbing isn't reimplemented in each module that touches the
registry.
"""
from dataclasses import dataclass
from typing import List, Optional, Tuple

import httpx

from .constants import (
    CHUNK_COUNT_KEY,
    CHUNK_MEDIA_TYPE,
    DEFAULT_HTTP_TIMEOUT,
    FILE_DIGEST_KEY,
    FILE_SIZE_KEY,
    KNOWN_LAYOUTS,
    LAYER_TITLE_KEY,
    LAYOUT_ANNOTATION_KEY,
    OCI_MANIFEST_ACCEPT,
    POINTER_MEDIA_TYPE,
)
from .errors import (
    MalformedManifestError,
    RevisionNotFoundError,
    UnsupportedLayoutError,
)


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


def _guard_layout(manifest: dict) -> None:
    """Refuse a manifest whose `com.hippius.layout` this build cannot read.

    The forward-compatibility floor. An artifact tagged with a layout value
    outside `KNOWN_LAYOUTS` was written by a newer client; reading it with this
    build's assumptions would misresolve files. Every pre-chunking artifact has
    no such annotation and passes untouched. Centralized in `fetch_manifest` so
    that single chokepoint protects every read path (repo_info, list_repo_files,
    file_exists, hf_hub_download, and the snapshot manifest cache seeded here).
    """
    layout = manifest.get("annotations", {}).get(LAYOUT_ANNOTATION_KEY)
    if layout is not None and layout not in KNOWN_LAYOUTS:
        raise UnsupportedLayoutError(
            f"manifest declares {LAYOUT_ANNOTATION_KEY}={layout!r}, which this "
            "hippius-hub build cannot read; upgrade hippius-hub to a version that "
            "supports this artifact layout."
        )


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
    manifest = resp.json()
    _guard_layout(manifest)
    return ManifestResult(manifest=manifest, digest=resp.headers.get("Docker-Content-Digest"))


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


@dataclass(frozen=True)
class ChunkRef:
    """One content-defined chunk blob of a chunked file, in file order.

    `digest` builds the blob URL and verifies the pulled bytes; `size` gives the
    write offset (sum of preceding chunk sizes) for a parallel, in-place assemble.
    """

    digest: str
    size: int


@dataclass(frozen=True)
class FileGroup:
    """One logical file in a manifest, independent of its physical layout.

    A plain (pre-chunking / small) file has `chunks == ()` and carries its own
    blob `digest`/`size`. A chunked file has K `chunks` and carries the
    *whole-file* `digest`/`size` (from the pointer layer's annotations, not the
    pointer blob's own tiny digest) — so `siblings`, `list_repo_files`, and the
    downloader all see the logical file, never the pointer/chunk plumbing.
    """

    title: str
    size: Optional[int]
    digest: Optional[str]
    chunks: Tuple[ChunkRef, ...]

    @property
    def is_chunked(self) -> bool:
        return bool(self.chunks)


def _pointer_group(title: str, pointer: dict, following: list) -> Tuple[FileGroup, int]:
    """Build a chunked FileGroup from a pointer layer + the layers after it.

    Consumes exactly `com.hippius.chunk.count` untitled chunk layers immediately
    following the pointer (they are emitted contiguously and in order by the
    uploader; OCI preserves layer order in the content-addressed manifest).
    Returns the group and how many layers it consumed *after* the pointer.
    """
    ann = pointer.get("annotations", {})
    try:
        count = int(ann[CHUNK_COUNT_KEY])
        file_size = int(ann[FILE_SIZE_KEY])
        file_digest = ann[FILE_DIGEST_KEY]
    except (KeyError, ValueError, TypeError) as exc:
        raise MalformedManifestError(
            f"chunked pointer layer {title!r} is missing or has a non-integer "
            f"{CHUNK_COUNT_KEY}/{FILE_SIZE_KEY}/{FILE_DIGEST_KEY} annotation"
        ) from exc

    # A pointer must reference at least one chunk. A count of 0 would yield a
    # group with no chunks, which `is_chunked` would then treat as a plain K=1
    # file whose blob (the whole-file digest) was never uploaded — a 404 on
    # download. Reject it as malformed rather than silently degrade.
    if count < 1:
        raise MalformedManifestError(
            f"chunked pointer layer {title!r} declares {CHUNK_COUNT_KEY}={count} "
            "(must be >= 1)"
        )

    chunks: List[ChunkRef] = []
    for layer in following:
        if len(chunks) == count:
            break
        # A titled layer marks the start of the next file: the chunk run ended
        # early, so the pointer's promised count is unmet -> malformed.
        if layer_title(layer) or layer.get("mediaType") != CHUNK_MEDIA_TYPE:
            break
        chunks.append(ChunkRef(digest=layer["digest"], size=layer["size"]))

    if len(chunks) != count:
        raise MalformedManifestError(
            f"chunked pointer layer {title!r} promises {count} chunk(s) but only "
            f"{len(chunks)} contiguous chunk layer(s) follow it"
        )
    return FileGroup(title=title, size=file_size, digest=file_digest, chunks=tuple(chunks)), count


def group_files(manifest: dict) -> List[FileGroup]:
    """Collapse a manifest's layers into one FileGroup per logical file.

    Walks layers in order. A titled pointer layer plus its trailing untitled
    chunk layers become one chunked group; any other titled layer is a plain
    K=1 file; stray untitled layers (a chunk with no preceding pointer) are a
    malformed manifest. This is the single read-side chokepoint every consumer
    (siblings, list_repo_files, file_exists, snapshot, single-file download)
    routes through, so they all agree on what "a file" is regardless of layout.
    """
    layers = manifest.get("layers", [])
    groups: List[FileGroup] = []
    i = 0
    while i < len(layers):
        layer = layers[i]
        title = layer_title(layer)
        if not title:
            raise MalformedManifestError(
                f"untitled layer at index {i} ({layer.get('mediaType')!r}) has no "
                "preceding pointer layer to attach to"
            )
        if layer.get("mediaType") == POINTER_MEDIA_TYPE:
            group, consumed = _pointer_group(title, layer, layers[i + 1:])
            groups.append(group)
            i += 1 + consumed
        else:
            groups.append(
                FileGroup(title=title, size=layer.get("size"), digest=layer.get("digest"), chunks=())
            )
            i += 1
    return groups


def layer_titles(manifest: dict) -> List[str]:
    """Return the in-repo filename of every logical file in a manifest.

    One entry per file (a chunked file appears once, under its pointer's title —
    its untitled chunk layers never surface here), so callers enumerating files
    (`list_repo_files`, `file_exists`, `snapshot_download`) see the logical view.
    """
    return [group.title for group in group_files(manifest)]
