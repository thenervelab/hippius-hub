"""Low-level OCI distribution helpers shared across hippius_hub modules.

Centralizes manifest fetch, layer iteration, and the OCI v2 accept header
so the same plumbing isn't reimplemented in each module that touches the
registry.
"""
import json
from dataclasses import dataclass
from typing import List, Optional, Tuple

import httpx

from .constants import (
    CHUNK_COUNT_KEY,
    CHUNKED_LAYOUT_V2,
    DEFAULT_HTTP_TIMEOUT,
    FILE_DIGEST_KEY,
    FILE_SIZE_KEY,
    KNOWN_LAYOUTS,
    LAYER_TITLE_KEY,
    LAYOUT_ANNOTATION_KEY,
    OCI_MANIFEST_ACCEPT,
    POINTER_MEDIA_TYPE_V2,
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
class PackChunkRef:
    """One chunk of a chunked-v2 file, located inside a pack blob, in file order.

    `chunk_digest`/`size` verify the chunk's bytes and give its whole-file write
    offset (sum of preceding sizes). `pack_digest`+`pack_offset` say which pack
    blob holds the bytes and where — so the downloader fetches packs (whole or
    ranged) and slices each chunk out. Many chunks share a pack, and a re-upload
    references old packs by range.
    """

    chunk_digest: str
    size: int
    pack_digest: str
    pack_offset: int


@dataclass(frozen=True)
class FileGroup:
    """One logical file in a manifest, independent of its physical layout.

    Two layouts, one view: a *plain* file (`layout is None`) carries its own
    whole-file blob `digest`/`size`; a *chunked-v2* file (`layout ==
    CHUNKED_LAYOUT_V2`) carries `pointer_digest` — the pack→chunk mapping lives in
    that pointer BLOB, fetched on download, not in the manifest. In both cases the
    whole-file `size`/`digest` come from the annotations (v2) or the layer (plain),
    so `siblings`/`list_repo_files` stay a pure manifest read — no pointer fetch on
    the metadata path.
    """

    title: str
    size: Optional[int]
    digest: Optional[str]
    layout: Optional[str] = None
    pointer_digest: Optional[str] = None

    @property
    def is_chunked(self) -> bool:
        # A v2 group has a layout set; the pack mapping is resolved from the
        # pointer blob on download, not carried in the manifest.
        return self.layout is not None


def _pointer_group_v2(title: str, pointer: dict) -> FileGroup:
    """Build a chunked-v2 FileGroup from a `pointer.v2` layer alone.

    The chunk→pack mapping is NOT positional in the manifest — it lives in the
    pointer BLOB (fetched on download via `pointer_digest`). Here we only read the
    whole-file annotations (so the metadata path never fetches the blob) and carry
    the pointer's own digest. The pack layers are untitled and are skipped by
    `group_files`; they are resolved by the pointer, not by position.
    """
    ann = pointer.get("annotations", {})
    try:
        count = int(ann[CHUNK_COUNT_KEY])
        file_size = int(ann[FILE_SIZE_KEY])
        file_digest = ann[FILE_DIGEST_KEY]
    except (KeyError, ValueError, TypeError) as exc:
        raise MalformedManifestError(
            f"chunked-v2 pointer layer {title!r} is missing or has a non-integer "
            f"{CHUNK_COUNT_KEY}/{FILE_SIZE_KEY}/{FILE_DIGEST_KEY} annotation"
        ) from exc
    if count < 1:
        raise MalformedManifestError(
            f"chunked-v2 pointer layer {title!r} declares {CHUNK_COUNT_KEY}={count} "
            "(must be >= 1)"
        )
    return FileGroup(
        title=title, size=file_size, digest=file_digest,
        layout=CHUNKED_LAYOUT_V2, pointer_digest=pointer["digest"],
    )


def parse_pointer_v2(blob: bytes) -> Tuple[PackChunkRef, ...]:
    """Parse a fetched chunked-v2 pointer blob into ordered pack-chunk refs.

    Pure: verifying the blob against its layer digest is the caller's job (done in
    the downloader). Raises `MalformedManifestError` on any structural violation —
    reassembling from a malformed pointer would produce a wrong/truncated file.
    """
    try:
        doc = json.loads(blob)
        if doc.get("version") != CHUNKED_LAYOUT_V2:
            raise MalformedManifestError(
                f"pointer blob version is {doc.get('version')!r}, expected "
                f"{CHUNKED_LAYOUT_V2!r}"
            )
        refs = tuple(
            PackChunkRef(
                chunk_digest=c["digest"],
                size=int(c["size"]),
                pack_digest=c["pack"],
                pack_offset=int(c["offset"]),
            )
            for c in doc["chunks"]
        )
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise MalformedManifestError(f"malformed chunked-v2 pointer blob: {exc}") from exc
    if not refs:
        raise MalformedManifestError("chunked-v2 pointer blob has no chunks")
    return refs


def group_files(manifest: dict) -> List[FileGroup]:
    """Collapse a manifest's layers into one FileGroup per logical file.

    Walks layers in order. A titled `pointer.v2` layer becomes one chunked group;
    any other titled layer is a plain K=1 file. Untitled layers — the pack blobs a
    pointer references, or third-party content co-located in the repo (a
    `docker`/`oras` push) — are skipped, so a foreign manifest degrades to its
    titled subset instead of hard-failing every read API, matching the pre-chunking
    reader's silent-skip behavior. This is the single read-side chokepoint every
    consumer (siblings, list_repo_files, file_exists, snapshot, single-file
    download) routes through, so they all agree on what "a file" is regardless of
    layout.
    """
    layers = manifest.get("layers", [])
    groups: List[FileGroup] = []
    i = 0
    while i < len(layers):
        layer = layers[i]
        title = layer_title(layer)
        if not title:
            # Untitled: a pack blob (resolved via its pointer, not by position) or
            # foreign tooling's layer. Skip either way — don't break reads of a
            # co-located non-hippius manifest.
            i += 1
            continue
        if layer.get("mediaType") == POINTER_MEDIA_TYPE_V2:
            groups.append(_pointer_group_v2(title, layer))
        else:
            groups.append(
                FileGroup(title=title, size=layer.get("size"), digest=layer.get("digest"))
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
