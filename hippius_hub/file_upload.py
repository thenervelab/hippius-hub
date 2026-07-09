"""Upload path: `upload_file` / `upload_folder` against the OCI registry.

Hashes each file, pushes its blob via the Rust extension, then merges the
layer list into the existing manifest and PUTs it with `If-Match` so a
concurrent writer's manifest can't be silently clobbered. Folder uploads
parallelise per-file via a ThreadPoolExecutor.
"""
import datetime
import hashlib
import json
import os
import tempfile
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import BinaryIO, Dict, List, Optional, Union

import httpx
from huggingface_hub import CommitInfo
from huggingface_hub.utils import filter_repo_objects
from tqdm import tqdm

from ._oci import fetch_manifest, layer_title
from .auth import get_oci_bearer_token
from .constants import DEFAULT_HTTP_TIMEOUT, LAYER_TITLE_KEY, resolve_registry
from .errors import ConcurrentManifestUpdateError, ManifestTooLargeError
from .auth import get_oci_bearer_token, get_token, resolve_token_value
from .constants import (
    ARTIFACT_TYPE_CHUNKED,
    CHUNK_COUNT_KEY,
    CHUNK_MEDIA_TYPE,
    CHUNKED_LAYOUT,
    DEFAULT_HTTP_TIMEOUT,
    FILE_DIGEST_KEY,
    FILE_SIZE_KEY,
    LAYER_TITLE_KEY,
    LAYOUT_ANNOTATION_KEY,
    MAX_MANIFEST_BYTES,
    POINTER_MEDIA_TYPE,
    resolve_cdc_avg_size,
    resolve_chunk_threshold,
    resolve_chunked_write_enabled,
    resolve_registry,
    resolve_upload_workers,
)
from .file_download import _oci_repo_path, _validate_repo_type

try:
    from .hippius_core import (
        chunk_and_hash_native,
        hash_file_native,
        upload_blob_native,
        upload_blob_range_native,
    )
except ImportError:
    raise ImportError("hippius_core is not installed. Did you run `maturin develop`?")


# ---- helpers ----

def _oci_bearer(repo_id: str, token, push: bool = True, endpoint=None) -> str:
    # Token resolution + the off-origin credential guard happen inside
    # get_oci_bearer_token, which mints from `resolve_registry(endpoint)`.
    return get_oci_bearer_token(repo_id, token, push=push, endpoint=endpoint)


def _empty_config_blob_descriptor() -> tuple:
    data = b"{}"
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
    return data, digest, len(data)


def _upload_blob_single_put(registry: str, repo_id: str, oci_token: str, file_path: str, digest: str) -> None:
    """OCI blob-upload init against the registry, then one streaming PUT-with-digest
    straight to it. This is the path for a plain (sub-threshold) whole-file blob;
    large files go through the chunked path instead (`_upload_file_chunked`)."""
    headers = {"Authorization": f"Bearer {oci_token}"}
    init_headers = {**headers, "Content-Length": "0"}
    init = httpx.post(f"{registry}/v2/{repo_id}/blobs/uploads/", headers=init_headers, timeout=DEFAULT_HTTP_TIMEOUT)
    init.raise_for_status()
    location = init.headers.get("Location")
    if not location:
        raise ValueError("Registry did not return a Location header for upload initiation")
    if location.startswith("/"):
        location = f"{registry}{location}"
    sep = "&" if "?" in location else "?"
    upload_blob_native(f"{location}{sep}digest={digest}", file_path, oci_token)


def _ensure_blob_uploaded(
    registry: str,
    repo_id: str,
    oci_token: str,
    file_path: str,
    sha256_hash: str,
) -> bool:
    """Upload a plain whole-file blob if not already present at its digest.
    Returns True if a new upload happened, False if the blob already existed and
    was skipped (the dedup HEAD)."""
    digest = f"sha256:{sha256_hash}"
    headers = {"Authorization": f"Bearer {oci_token}"}
    check = httpx.head(f"{registry}/v2/{repo_id}/blobs/{digest}", headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
    if check.status_code == 200:
        return False
    _upload_blob_single_put(registry, repo_id, oci_token, file_path, digest)
    return True


def _ensure_config_blob_uploaded(registry: str, repo_id: str, oci_token: str) -> tuple:
    """Push the empty-object config blob if missing. Returns (digest, size)."""
    data, digest, size = _empty_config_blob_descriptor()
    headers = {"Authorization": f"Bearer {oci_token}"}
    check = httpx.head(f"{registry}/v2/{repo_id}/blobs/{digest}", headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
    if check.status_code != 200:
        init = httpx.post(
            f"{registry}/v2/{repo_id}/blobs/uploads/",
            headers={**headers, "Content-Length": "0"},
            timeout=DEFAULT_HTTP_TIMEOUT,
        )
        init.raise_for_status()
        loc = init.headers.get("Location")
        if loc and loc.startswith("/"):
            loc = f"{registry}{loc}"
        sep = "&" if "?" in loc else "?"
        # Raise on a failed config PUT: otherwise the manifest PUT that follows
        # fails later with an opaque MANIFEST_BLOB_UNKNOWN instead of the real
        # cause (matches _ensure_bytes_blob_uploaded / _upload_blob_single_put).
        put = httpx.put(
            f"{loc}{sep}digest={digest}",
            headers={**headers, "Content-Type": "application/octet-stream"},
            content=data,
            timeout=DEFAULT_HTTP_TIMEOUT,
        )
        put.raise_for_status()
    return digest, size


def _ensure_bytes_blob_uploaded(registry: str, repo_id: str, oci_token: str, data: bytes, digest: str) -> None:
    """Push an in-memory blob (the pointer blob) if not already present at its
    digest. HEAD-dedups first, then runs the OCI init + PUT-with-digest dance."""
    headers = {"Authorization": f"Bearer {oci_token}"}
    check = httpx.head(f"{registry}/v2/{repo_id}/blobs/{digest}", headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
    if check.status_code == 200:
        return
    init = httpx.post(f"{registry}/v2/{repo_id}/blobs/uploads/", headers={**headers, "Content-Length": "0"}, timeout=DEFAULT_HTTP_TIMEOUT)
    init.raise_for_status()
    location = init.headers.get("Location")
    if not location:
        raise ValueError("Registry did not return a Location header for upload initiation")
    if location.startswith("/"):
        location = f"{registry}{location}"
    sep = "&" if "?" in location else "?"
    put = httpx.put(f"{location}{sep}digest={digest}", headers={**headers, "Content-Type": "application/octet-stream"}, content=data, timeout=DEFAULT_HTTP_TIMEOUT)
    put.raise_for_status()


def _ensure_chunk_uploaded(registry: str, repo_id: str, oci_token: str, abs_path: str, digest: str, offset: int, size: int) -> None:
    """Upload one content-defined chunk's byte range if absent at its digest.

    The `HEAD` is the 'upload only the bytes we're missing' core: a chunk that a
    prior upload already stored (an unchanged region of a re-uploaded file) is
    skipped, so a slightly-changed large file re-transfers only its changed
    chunks. Missing chunks stream straight to Harbor via the Rust range PUT."""
    headers = {"Authorization": f"Bearer {oci_token}"}
    check = httpx.head(f"{registry}/v2/{repo_id}/blobs/{digest}", headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
    if check.status_code == 200:
        return
    init = httpx.post(f"{registry}/v2/{repo_id}/blobs/uploads/", headers={**headers, "Content-Length": "0"}, timeout=DEFAULT_HTTP_TIMEOUT)
    init.raise_for_status()
    location = init.headers.get("Location")
    if not location:
        raise ValueError("Registry did not return a Location header for upload initiation")
    if location.startswith("/"):
        location = f"{registry}{location}"
    sep = "&" if "?" in location else "?"
    upload_blob_range_native(
        url=f"{location}{sep}digest={digest}",
        path=abs_path,
        offset=offset,
        length=size,
        auth_token=oci_token,
    )


def _pointer_blob_bytes(whole_hex: str, file_size: int, chunk_metas: list) -> bytes:
    """Serialize the deterministic pointer blob for a chunked file.

    Content = layout version + whole-file size/digest + the ordered chunk list.
    Canonical JSON (sorted keys, no whitespace, NO timestamps) so two identical
    files produce the same pointer digest and dedup at the pointer level too. It
    is self-identifying (the `version` field) so any reader that predates chunked
    support and writes this blob verbatim as the file fails an obvious size/digest
    check rather than corrupting silently."""
    doc = {
        "version": CHUNKED_LAYOUT,
        "file": {"size": file_size, "digest": f"sha256:{whole_hex}"},
        "chunks": [{"digest": f"sha256:{h}", "size": size} for h, _offset, size in chunk_metas],
    }
    return json.dumps(doc, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _upload_file_chunked(abs_path: str, repo_title: str, file_size: int, registry: str, oci_repo: str, oci_token: str) -> List[dict]:
    """Store a large file as a pointer + K content-defined chunk blobs.

    Returns the layer list for this file: one titled pointer layer followed by K
    untitled chunk layers, in file order. Chunks are HEAD-deduped and pushed
    concurrently (the WAN-parallelism win); the pointer blob is written last so a
    crash before it leaves only unreferenced chunk blobs for Harbor GC to reclaim,
    never a half-referenced manifest."""
    whole_hex, chunk_metas = chunk_and_hash_native(abs_path, resolve_cdc_avg_size())

    def _one(meta) -> dict:
        chunk_hex, offset, size = meta
        digest = f"sha256:{chunk_hex}"
        _ensure_chunk_uploaded(registry, oci_repo, oci_token, abs_path, digest, offset, size)
        return {"mediaType": CHUNK_MEDIA_TYPE, "size": size, "digest": digest}

    # ThreadPoolExecutor.map preserves input order, so chunk layers stay in file
    # order — which the reader relies on to assemble the file correctly.
    with ThreadPoolExecutor(max_workers=resolve_upload_workers()) as executor:
        chunk_layers = list(executor.map(_one, chunk_metas))

    pointer_bytes = _pointer_blob_bytes(whole_hex, file_size, chunk_metas)
    pointer_digest = f"sha256:{hashlib.sha256(pointer_bytes).hexdigest()}"
    _ensure_bytes_blob_uploaded(registry, oci_repo, oci_token, pointer_bytes, pointer_digest)

    pointer_layer = {
        "mediaType": POINTER_MEDIA_TYPE,
        "size": len(pointer_bytes),
        "digest": pointer_digest,
        "annotations": {
            LAYER_TITLE_KEY: repo_title.replace("\\", "/"),
            FILE_SIZE_KEY: str(file_size),
            FILE_DIGEST_KEY: f"sha256:{whole_hex}",
            CHUNK_COUNT_KEY: str(len(chunk_metas)),
        },
    }
    return [pointer_layer, *chunk_layers]


def _upload_file_layers(abs_path: str, repo_title: str, registry: str, oci_repo: str, oci_token: str) -> List[dict]:
    """Upload one file and return its manifest layer(s).

    A file at or above the chunk threshold is stored chunked (pointer + K chunks)
    unless the rollout gate (`HIPPIUS_CHUNKED_WRITE=0`) forces plain uploads;
    below the threshold, one plain blob — byte-identical to the pre-chunking
    layout, so small files and existing artifacts cross-dedup unchanged."""
    file_size = os.path.getsize(abs_path)
    if file_size >= resolve_chunk_threshold() and resolve_chunked_write_enabled():
        return _upload_file_chunked(abs_path, repo_title, file_size, registry, oci_repo, oci_token)
    sha256_hash, size = hash_file_native(abs_path)
    _ensure_blob_uploaded(registry, oci_repo, oci_token, abs_path, sha256_hash)
    return [_build_layer(sha256_hash, size, repo_title)]


def _prev_digest_or_warn(existing, repo_id: str, revision: str) -> Optional[str]:
    """Return the prior manifest's digest for If-Match, or None with a warning.

    If `existing` is None (fresh repo / 404 fetch), return None silently —
    there is no prior writer to race. If `existing.digest` is None (the
    registry honored the fetch but omitted Docker-Content-Digest), warn
    loudly: the next PUT will proceed without optimistic-concurrency
    protection, regressing this revision to last-writer-wins. Per OCI
    Distribution Spec §4.4.1, Docker-Content-Digest is RECOMMENDED but not
    REQUIRED on manifest responses, so some registries / stripping proxies
    legitimately omit it; we still want operators to see when they are
    running unprotected — silent regression is the failure mode the audit
    H1 fix was meant to close.
    """
    if existing is None:
        return None
    if existing.digest is None:
        warnings.warn(
            f"manifest fetch for {repo_id}:{revision} returned no "
            f"Docker-Content-Digest header; PUT will proceed without If-Match "
            f"and concurrent writers may silently overwrite each other",
            UserWarning,
            stacklevel=3,
        )
        return None
    return existing.digest


def _put_manifest(
    registry: str,
    repo_id: str,
    revision: str,
    oci_token: str,
    manifest: dict,
    *,
    if_match: Optional[str] = None,
) -> httpx.Response:
    """PUT the manifest to revision. Returns the response (with digest in headers).

    When `if_match` is provided (the digest from a prior fetch_manifest call),
    sends `If-Match: <digest>` per OCI distribution spec 4.4. The server then
    rejects with 412 Precondition Failed if a concurrent writer has advanced
    the revision in between — we surface that as ConcurrentManifestUpdateError
    so callers can choose to retry or serialize externally rather than silently
    overwriting the other writer's layer.
    """
    url = f"{registry}/v2/{repo_id}/manifests/{revision}"
    headers = {
        "Authorization": f"Bearer {oci_token}",
        "Content-Type": "application/vnd.oci.image.manifest.v1+json",
    }
    if if_match:
        headers["If-Match"] = if_match
    resp = httpx.put(url, headers=headers, json=manifest, timeout=DEFAULT_HTTP_TIMEOUT * 2)
    if resp.status_code == 412:
        raise ConcurrentManifestUpdateError(
            f"manifest at {repo_id}:{revision} changed between read and write",
            response=resp,
        )
    resp.raise_for_status()
    return resp


def _normalize_path_or_fileobj(path_or_fileobj) -> tuple:
    """Coerce HF's path_or_fileobj (str/Path/bytes/BinaryIO) into (filesystem_path, cleanup).
    cleanup() must be called after use; it's a no-op for real paths.

    Try/except here is load-bearing: a partial write (disk full, encoding error)
    must not leak a temp file. The caller pattern is `path, cleanup = ...; try: use(path); finally: cleanup()`.
    """
    if isinstance(path_or_fileobj, (str, Path)):
        return str(path_or_fileobj), lambda: None

    if not (isinstance(path_or_fileobj, bytes) or hasattr(path_or_fileobj, "read")):
        raise TypeError(
            f"path_or_fileobj must be str/Path/bytes/BinaryIO, got {type(path_or_fileobj).__name__}"
        )

    chunk_size = 4 * 1024 * 1024
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
    try:
        if isinstance(path_or_fileobj, bytes):
            tmp.write(path_or_fileobj)
        else:
            # Stream-read so multi-GB BinaryIO inputs don't materialize in memory.
            while True:
                chunk = path_or_fileobj.read(chunk_size)
                if not chunk:
                    break
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                tmp.write(chunk)
        tmp.flush()
    except BaseException:
        tmp.close()
        if os.path.exists(tmp.name):
            os.unlink(tmp.name)
        raise
    finally:
        if not tmp.closed:
            tmp.close()

    tmp_path = tmp.name
    return tmp_path, lambda: os.path.exists(tmp_path) and os.unlink(tmp_path)


def _build_layer(sha256_hash: str, file_size: int, path_in_repo: str) -> dict:
    return {
        "mediaType": "application/octet-stream",
        "size": file_size,
        "digest": f"sha256:{sha256_hash}",
        "annotations": {
            LAYER_TITLE_KEY: path_in_repo.replace("\\", "/"),
        },
    }


def _partition_groups(layers: List[dict]) -> List[tuple]:
    """Split a layer list into (title, [layers]) file-groups.

    A titled layer starts a group; untitled chunk layers attach to the preceding
    group. So a chunked file's pointer + K chunk layers are one indivisible unit:
    dropping it by title drops its chunks too, and keeping it keeps them all.
    This is what makes `_merge_layers` group-aware — the fix for the data-loss
    bug where a title-keyed merge either collapsed a chunked file to its pointer
    or wiped its chunk layers when an unrelated file was committed."""
    groups: List[tuple] = []
    for layer in layers:
        title = layer_title(layer)
        if title or not groups:
            groups.append((title, [layer]))
        else:
            groups[-1][1].append(layer)
    return groups


def _merge_layers(
    existing: List[dict],
    new_layers: List[dict],
    delete_titles: Optional[set] = None,
) -> List[dict]:
    """Combine `existing` with `new_layers` at file-group granularity.

    An existing file-group is dropped when its title is being replaced by
    `new_layers` or is in `delete_titles`; every surviving group is preserved
    INTACT (pointer + all its chunk layers), then the new layers are appended.
    A file's group is never partially rewritten, so committing one file can't
    damage another chunked file, and replacing a chunked file swaps its whole
    group. For plain single-layer files this reduces to the old title-keyed
    behavior (new replaces same-title, deletes drop, others preserved)."""
    delete_titles = delete_titles or set()
    new_titles = {title for title, _ in _partition_groups(new_layers) if title}
    result: List[dict] = []
    for title, group_layers in _partition_groups(existing):
        if title is not None and (title in delete_titles or title in new_titles):
            continue
        result.extend(group_layers)
    result.extend(new_layers)
    return result


def _assemble_manifest(
    config_digest: str,
    config_size: int,
    merged_layers: List[dict],
    commit_message: Optional[str],
    commit_description: Optional[str],
) -> dict:
    """Build the OCI manifest, typing it as a chunked artifact only when it holds
    at least one chunked file.

    A manifest with any pointer layer gets `artifactType` (so image tooling / Trivy
    treat it as a generic artifact, not a broken image) and the
    `com.hippius.layout` annotation (so a layout-blind client hits the Phase 0
    guard). A purely-plain manifest stays byte-identical to the pre-chunking
    output, preserving cross-dedup with existing artifacts."""
    annotations = _commit_annotations(commit_message, commit_description)
    manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": config_digest,
            "size": config_size,
        },
        "layers": merged_layers,
        "annotations": annotations,
    }
    if any(layer.get("mediaType") == POINTER_MEDIA_TYPE for layer in merged_layers):
        manifest["artifactType"] = ARTIFACT_TYPE_CHUNKED
        annotations[LAYOUT_ANNOTATION_KEY] = CHUNKED_LAYOUT
    _guard_manifest_size(manifest)
    return manifest


def _guard_manifest_size(manifest: dict) -> None:
    """Refuse a manifest that would exceed the registry's 4 MiB PUT cap.

    Checked here, before the blobs' manifest is PUT, so a too-large artifact
    fails with a clear error instead of the registry's opaque 400 after every
    blob is already uploaded. Serialized the same way httpx sends it (default
    `json.dumps` + utf-8) so the measured size matches the wire body."""
    size = len(json.dumps(manifest).encode("utf-8"))
    if size > MAX_MANIFEST_BYTES:
        raise ManifestTooLargeError(
            f"manifest is {size} bytes with {len(manifest['layers'])} layers, over the "
            f"{MAX_MANIFEST_BYTES}-byte registry limit; this artifact has too many chunks "
            "for a single manifest (Referrers/index fan-out is a planned follow-up)."
        )


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


def _upload_one_file(
    *,
    rel_path: str,
    base_dir: str,
    path_in_repo: Optional[str],
    registry: str,
    oci_repo: str,
    oci_token: str,
) -> List[dict]:
    """Upload one file from a folder and return its manifest layer(s).

    Returns a LIST because a chunked file contributes a pointer layer plus K
    chunk layers (a plain file contributes one). Extracted from the per-file
    closure in `upload_folder` so the thread-pool body is testable in isolation.
    """
    abs_path = os.path.join(base_dir, rel_path)
    repo_title = f"{path_in_repo}/{rel_path}" if path_in_repo else rel_path
    tqdm.write(f"🚀 Uploading: {repo_title} ({os.path.getsize(abs_path)} bytes)...")
    layers = _upload_file_layers(abs_path, repo_title, registry, oci_repo, oci_token)
    tqdm.write(f"✅ Uploaded: {repo_title}")
    return layers


def _finalize_upload_manifest(
    *,
    registry: str,
    oci_repo: str,
    oci_token: str,
    repo_id: str,
    revision: str,
    new_layers: List[dict],
    delete_patterns: Optional[Union[List[str], str]],
    commit_message: str,
    commit_description: str,
) -> CommitInfo:
    """Merge new layers into the existing manifest and PUT it (with If-Match).

    Single read-modify-write on the manifest: one fetch reused for both the
    delete-title computation and the merge, the captured digest threaded back
    into the PUT as `If-Match` so a racing writer surfaces as 412 →
    `ConcurrentManifestUpdateError` rather than silent last-writer-wins. The
    deliberate non-reuse from `upload_file` is documented on `_upload_one_file`.
    """
    existing = fetch_manifest(registry, oci_repo, revision, oci_token, missing_ok=True)
    existing_layers = existing.manifest.get("layers", []) if existing else []
    prev_digest = _prev_digest_or_warn(existing, repo_id, revision)

    delete_titles = set()
    if delete_patterns:
        existing_titles = [t for t in (layer_title(l) for l in existing_layers) if t]
        delete_titles = set(filter_repo_objects(items=existing_titles, allow_patterns=delete_patterns))

    merged_layers = _merge_layers(existing_layers, new_layers, delete_titles=delete_titles)

    config_digest, config_size = _ensure_config_blob_uploaded(registry, oci_repo, oci_token)
    manifest = _assemble_manifest(
        config_digest, config_size, merged_layers, commit_message, commit_description
    )

    print(f"📝 Publishing OCI Manifest for {revision}...")
    resp = _put_manifest(registry, oci_repo, revision, oci_token, manifest, if_match=prev_digest)
    return _build_commit_info(registry, repo_id, revision, resp, commit_message, commit_description)


def _handle_unsupported_kwargs(create_pr, parent_commit, run_as_future):
    """`create_pr` and `parent_commit` are accept-and-warn (closest HF analog is
    "no PR concept" and "no optimistic concurrency" — the upload still proceeds).
    `run_as_future` would require returning a Future we can't fulfill — raise."""
    if create_pr:
        warnings.warn(
            "create_pr=True is ignored: Hippius has no pull-request concept; "
            "the upload writes directly to the revision.",
            UserWarning,
            stacklevel=3,
        )
    if parent_commit:
        warnings.warn(
            "parent_commit is ignored: Hippius revisions are OCI tags without "
            "an HF-style optimistic-concurrency check.",
            UserWarning,
            stacklevel=3,
        )
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

    Concurrency: this is a read-modify-write on the manifest. We send
    `If-Match: <previous-digest>` on the PUT so the registry rejects (412 →
    `ConcurrentManifestUpdateError`) when a racing writer has advanced the
    revision between our fetch and our PUT — the alternative (silent
    last-writer-wins) loses the racing writer's layer. Callers receive the
    typed exception and can retry or serialize externally.
    """
    _validate_repo_type(repo_type)
    _handle_unsupported_kwargs(create_pr, parent_commit, run_as_future)
    if revision is None:
        revision = "main"
    if commit_message is None:
        commit_message = f"Upload {path_in_repo}"
    if commit_description is None:
        commit_description = ""

    oci_repo = _oci_repo_path(repo_id, repo_type)
    registry = resolve_registry(endpoint)
    oci_token = _oci_bearer(oci_repo, token, push=True, endpoint=endpoint)

    file_path, cleanup = _normalize_path_or_fileobj(path_or_fileobj)
    try:
        new_layers = _upload_file_layers(file_path, path_in_repo, registry, oci_repo, oci_token)
    finally:
        cleanup()

    existing = fetch_manifest(registry, oci_repo, revision, oci_token, missing_ok=True)
    existing_layers = existing.manifest.get("layers", []) if existing else []
    prev_digest = _prev_digest_or_warn(existing, repo_id, revision)
    merged_layers = _merge_layers(existing_layers, new_layers)

    config_digest, config_size = _ensure_config_blob_uploaded(registry, oci_repo, oci_token)
    manifest = _assemble_manifest(
        config_digest, config_size, merged_layers, commit_message, commit_description
    )

    resp = _put_manifest(registry, oci_repo, revision, oci_token, manifest, if_match=prev_digest)
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
    max_workers: int = 8,
) -> CommitInfo:
    """Upload every file under `folder_path` to a repository revision.

    Honors HF allow_patterns/ignore_patterns/delete_patterns. Merges with the
    existing manifest — any layer with a matching title is replaced; titles
    matching delete_patterns are removed from the new manifest entirely.

    Concurrency: like `upload_file`, the PUT carries `If-Match` from the
    manifest fetch. A concurrent writer that advanced the revision in the
    meantime causes the registry to return 412, which surfaces here as
    `ConcurrentManifestUpdateError` — the partial-folder write does NOT
    silently land. Blob pushes that already completed are idempotent at the
    OCI level (content-addressed), so a retry of the whole folder is safe.

    `max_workers` controls the per-file ThreadPoolExecutor — mirrors the
    parameter on `snapshot_download` so callers with large folders and fast
    pipes can push the upload pool past the default of 8.
    """
    _validate_repo_type(repo_type)
    _handle_unsupported_kwargs(create_pr, parent_commit, run_as_future)
    if revision is None:
        revision = "main"
    if commit_message is None:
        commit_message = "Upload folder using hippius_hub"
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

    oci_repo = _oci_repo_path(repo_id, repo_type)
    registry = resolve_registry(endpoint)
    oci_token = _oci_bearer(oci_repo, token, push=True, endpoint=endpoint)

    new_layers = []
    if filtered:
        print(f"📦 Preparing to upload {len(filtered)} file(s) to {repo_id}:{revision}...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    _upload_one_file,
                    rel_path=rel,
                    base_dir=base_dir,
                    path_in_repo=path_in_repo,
                    registry=registry,
                    oci_repo=oci_repo,
                    oci_token=oci_token,
                )
                for rel in filtered
            ]
            for fut in tqdm(as_completed(futures), total=len(filtered), desc="Uploading", unit="file"):
                new_layers.extend(fut.result())

    commit_info = _finalize_upload_manifest(
        registry=registry,
        oci_repo=oci_repo,
        oci_token=oci_token,
        repo_id=repo_id,
        revision=revision,
        new_layers=new_layers,
        delete_patterns=delete_patterns,
        commit_message=commit_message,
        commit_description=commit_description,
    )
    # Count logical files uploaded, not manifest layers: a chunked file expands
    # to a pointer + K chunk layers, so len(new_layers) would overcount.
    print(f"🎉 Successfully pushed {len(filtered)} file(s) to {repo_id}:{revision}")
    return commit_info


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
