"""Single-file download path: `hf_hub_download` against the OCI registry.

Resolves a (repo_id, filename, revision) tuple to the matching layer digest
via the OCI manifest, then delegates the blob fetch to the Rust extension
(`hippius_core`). Mirrors huggingface_hub's local cache layout so the same
on-disk state is interchangeable.
"""
import os
import shutil
import tempfile
import uuid
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Union

from . import _http
from ._oci import FileGroup, fetch_manifest, group_files, parse_pointer_v2
from .auth import get_oci_bearer_token
from .constants import (
    DEFAULT_CACHE_DIR,
    DEFAULT_HTTP_TIMEOUT,
    PACK_MEDIA_TYPE,
    resolve_chunk_size,
    resolve_max_concurrent,
    resolve_registry,
    resolve_verify_hash,
)
from .errors import (
    EntryNotFoundError,
    LocalEntryNotFoundError,
    RevisionNotFoundError,
)

try:
    from .hippius_core import (
        download_file_native,
        download_packs_native,
    )
except ImportError:
    raise ImportError("hippius_core is not installed. Did you run `maturin develop`?")


_VALID_REPO_TYPES = (None, "model", "dataset", "space")


def _validate_repo_type(repo_type: Optional[str]):
    if repo_type not in _VALID_REPO_TYPES:
        raise NotImplementedError(
            f"repo_type={repo_type!r} is not supported by hippius_hub. "
            f"Valid values: {', '.join(repr(t) for t in _VALID_REPO_TYPES)}."
        )


def _handle_ignored_download_kwargs(
    *,
    etag_timeout: float,
    tqdm_class,
    dry_run: bool,
    headers,
    user_agent,
    library_name,
    library_version,
):
    """Emit UserWarning for HF kwargs we accept but don't yet honor.

    Use only from hf_hub_download. snapshot_download has its own variant
    (it implements dry_run, so doesn't raise for it). stacklevel=3 points
    the warning at the user's call site, not this helper.
    """
    # Compare against the documented HF default rather than truthiness:
    # etag_timeout=0 is a valid (if odd) user choice that should still warn.
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
    if dry_run:
        # dry_run is supported in snapshot_download but NOT here — fail fast
        # so users don't silently get a full download when they asked for a
        # no-op enumeration.
        raise NotImplementedError(
            "dry_run is not supported by hf_hub_download; use snapshot_download(dry_run=True) "
            "to enumerate files without downloading."
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


def _oci_repo_path(repo_id: str, repo_type: Optional[str]) -> str:
    """Translate (repo_id, repo_type) into the OCI-side repo path under `/v2/...`.

    Models keep their repo_id as-is (back-compat). Datasets and spaces are
    namespaced under the corresponding Harbor project so the same logical
    repo_id can exist across types without collision.

    Rejects already-prefixed repo_ids (`datasets/foo` with `repo_type="dataset"`)
    rather than producing a double-prefixed `datasets/datasets/foo` path.
    """
    if repo_type in (None, "model"):
        return repo_id
    if repo_type in ("dataset", "space"):
        prefix = f"{repo_type}s"  # datasets, spaces
        if repo_id.startswith(f"{prefix}/"):
            raise ValueError(
                f"repo_id={repo_id!r} already starts with {prefix + '/'!r}; "
                f"pass repo_id without the prefix when using repo_type={repo_type!r}"
            )
        return f"{prefix}/{repo_id}"
    # _validate_repo_type guards the only inputs that could reach here.
    # AssertionError (not NotImplementedError) is the canonical "unreachable
    # by design" signal — NotImplementedError reads as "TODO: implement",
    # which would be misleading: there is nothing left to implement here.
    raise AssertionError(
        f"unreachable: _validate_repo_type should have rejected {repo_type!r} before this point"
    )


def _cache_dirname(repo_id: str, repo_type: Optional[str]) -> str:
    """HF cache convention: `{kind}s--{namespace}--{name}` per repo type."""
    prefix = "models" if repo_type in (None, "model") else f"{repo_type}s"
    return f"{prefix}--{repo_id.replace('/', '--')}"


@dataclass(frozen=True)
class _DownloadPaths:
    """Cache-layout-resolved paths for a single file in hf_hub_download.

    `dest_file` is the final on-disk path of the downloaded file. When
    `local_dir` is set, only `dest_file` is meaningful (snapshots/repo_dir
    are not used). Otherwise the snapshots layout is populated for the
    cache.
    """

    dest_file: str
    # The next two are None when local_dir is set (we don't use cache layout).
    repo_dir: Optional[str]
    snapshots_dir: Optional[str]


def _safe_join(base: str, filename: str) -> str:
    """Join `filename` under `base`, rejecting paths that escape `base`.

    `filename` comes from the OCI manifest's `org.opencontainers.image.title`
    layer annotation, which is server-controlled. Without this guard an
    absolute path (`/etc/cron.d/x`) or a `..` segment would let a malicious or
    compromised manifest place files outside the cache / `local_dir` under the
    invoking user's permissions — the path-traversal vector huggingface_hub
    rejects in `_get_pointer_path`. Nested subdirectories (`weights/x.bin`,
    used for `subfolder=`) stay allowed; only escapes are refused.
    """
    # Split on both separators so a Windows-style `..\x` is caught on POSIX too
    # (os.path.join would otherwise treat the whole thing as one component).
    parts = filename.replace("\\", "/").split("/")
    if os.path.isabs(filename) or ".." in parts:
        raise ValueError(
            f"Unsafe path in repository file name {filename!r}: absolute paths "
            f"and '..' segments are not allowed (would escape {base!r})."
        )
    dest = os.path.join(base, *parts)
    # Backstop independent of the textual checks above: confirm the resolved
    # path still lives under `base`. Mirrors huggingface_hub's containment
    # assertion; `commonpath` is filesystem/drive-aware on every platform.
    base_abs = os.path.abspath(base)
    dest_abs = os.path.abspath(dest)
    try:
        contained = os.path.commonpath([base_abs, dest_abs]) == base_abs
    except ValueError:
        # Different drives (Windows) — commonpath raises; treat as not contained.
        contained = False
    if not contained:
        raise ValueError(
            f"Unsafe path in repository file name {filename!r}: resolves outside "
            f"the target directory {base!r}."
        )
    return dest


def _resolve_dest_paths(
    *,
    repo_id: str,
    filename: str,
    repo_type: Optional[str],
    revision: str,
    cache_dir: str,
    local_dir: Optional[Union[str, Path]],
) -> _DownloadPaths:
    """Compute where this file lands on disk given (local_dir vs cache_dir).

    `filename` is server-controlled (manifest layer title), so both the
    local_dir and cache destinations are routed through `_safe_join`, which
    refuses any path that escapes the target directory.
    """
    if local_dir is not None:
        return _DownloadPaths(
            dest_file=_safe_join(str(local_dir), filename),
            repo_dir=None,
            snapshots_dir=None,
        )
    repo_dir = os.path.join(cache_dir, _cache_dirname(repo_id, repo_type))
    snapshots_dir = os.path.join(repo_dir, "snapshots", revision)
    dest_file = _safe_join(snapshots_dir, filename)
    return _DownloadPaths(
        dest_file=dest_file, repo_dir=repo_dir, snapshots_dir=snapshots_dir
    )


def _digest_hex(digest: str) -> str:
    """Strip an OCI `algo:` prefix, yielding the bare hex the Rust layer verifies
    against and the cache uses as a blob name. Tolerates an already-bare hex."""
    return digest.split(":", 1)[-1]


def _resolve_file_group(
    manifest: Dict,
    filename: str,
    repo_id: str,
    revision: str,
):
    """Find the logical file named `filename` and return its FileGroup.

    Works across layouts: a plain file resolves to a K=0 group carrying its own
    blob digest; a chunked file resolves to its pointer group carrying the
    whole-file digest and the ordered chunk list. Preserves the old
    `_resolve_target_digest` contract — a match whose (plain) digest is falsy is
    treated as "not found" so callers never fetch a blob at an empty digest.
    """
    for group in group_files(manifest):
        if group.title == filename:
            if group.is_chunked or group.digest:
                return group
            break
    raise EntryNotFoundError(
        f"File '{filename}' not found in the OCI manifest of '{repo_id}:{revision}'"
    )


def _resolve_manifest(
    *,
    registry: str,
    oci_repo: str,
    revision: str,
    oci_token: str,
    cached: Optional[Dict],
) -> Dict:
    """Return the caller-provided manifest, or fetch it from the registry.

    `cached` is the `_resolved_manifest` kwarg of hf_hub_download — populated
    by snapshot_download to avoid N+1 manifest round-trips when downloading
    many files from the same repo:revision. Read path only: no If-Match.
    """
    if cached is not None:
        return cached
    return fetch_manifest(registry, oci_repo, revision, oci_token).manifest


def hf_hub_download(
    repo_id: str,
    filename: str,
    *,
    subfolder: Optional[str] = None,
    repo_type: Optional[str] = None,
    revision: Optional[str] = None,
    library_name: Optional[str] = None,
    library_version: Optional[str] = None,
    cache_dir: Optional[Union[str, Path]] = None,
    local_dir: Optional[Union[str, Path]] = None,
    user_agent: Union[Dict, str, None] = None,
    force_download: bool = False,
    etag_timeout: float = 10.0,
    token: Union[bool, str, None] = None,
    local_files_only: bool = False,
    headers: Optional[Dict[str, str]] = None,
    endpoint: Optional[str] = None,
    tqdm_class: Optional[type] = None,
    dry_run: bool = False,
    _resolved_manifest: Optional[Dict] = None,
    _oci_token: Optional[str] = None,
    _resolved_group: Optional[FileGroup] = None,
) -> str:
    """Drop-in replacement for huggingface_hub.hf_hub_download against an
    OCI-backed Hippius registry.

    Honored kwargs: subfolder, revision, cache_dir, local_dir, force_download,
    token, local_files_only, endpoint, repo_type (model-only).
    Accepted but currently ignored (UserWarning at call site):
        etag_timeout (when != 10.0), tqdm_class, headers, user_agent,
        library_name, library_version.
    Rejected (raises NotImplementedError): dry_run — supported in
        snapshot_download but not here, raise rather than silently download.

    hippius_hub-specific overrides via env: HIPPIUS_CHUNK_SIZE, HIPPIUS_VERIFY_HASH.
    """
    _validate_repo_type(repo_type)
    _handle_ignored_download_kwargs(
        etag_timeout=etag_timeout,
        tqdm_class=tqdm_class,
        dry_run=dry_run,
        headers=headers,
        user_agent=user_agent,
        library_name=library_name,
        library_version=library_version,
    )
    if force_download and local_files_only:
        raise ValueError(
            "Cannot pass 'force_download=True' and 'local_files_only=True' at the same time."
        )
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR
    cache_dir = str(cache_dir)
    if revision is None:
        revision = "main"
    if subfolder:
        filename = f"{subfolder}/{filename}"

    oci_repo = _oci_repo_path(repo_id, repo_type)
    paths = _resolve_dest_paths(
        repo_id=repo_id,
        filename=filename,
        repo_type=repo_type,
        revision=revision,
        cache_dir=cache_dir,
        local_dir=local_dir,
    )
    # 1. Cache check: never redownload an existing file
    if not force_download and os.path.exists(paths.dest_file):
        return paths.dest_file
    if local_files_only:
        raise LocalEntryNotFoundError(
            f"{filename!r} not found in local cache (cache_dir={cache_dir!r}) "
            f"and local_files_only=True"
        )

    registry = resolve_registry(endpoint)
    # _oci_token / _resolved_manifest / _resolved_group let snapshot_download skip
    # per-file token, manifest-fetch, and group-parse work (see each kwarg's note).
    # Token resolution + the off-origin credential guard happen inside
    # get_oci_bearer_token, which mints from `registry` (= resolve_registry(endpoint)).
    oci_token = _oci_token or get_oci_bearer_token(oci_repo, token, endpoint=endpoint)
    manifest = _resolve_manifest(
        registry=registry,
        oci_repo=oci_repo,
        revision=revision,
        oci_token=oci_token,
        cached=_resolved_manifest,
    )
    # snapshot_download resolves every file's group once from the shared manifest
    # and threads it here, so a snapshot doesn't re-run group_files (an O(layers)
    # parse) per file under the GIL. A direct caller passes nothing and resolves it.
    group = (
        _resolved_group
        if _resolved_group is not None
        else _resolve_file_group(manifest, filename, repo_id, revision)
    )
    # Re-assert _resolve_file_group's guard on the threaded path: a snapshot-supplied
    # group skips that call, so without this a titled plain layer with a falsy digest
    # would reach the plain path and build a `.../blobs/None` URL instead of raising
    # the same EntryNotFoundError a direct hf_hub_download would.
    if not group.is_chunked and not group.digest:
        raise EntryNotFoundError(
            f"File '{filename}' not found in the OCI manifest of '{repo_id}:{revision}'"
        )
    # Chunked files pull K content-addressed chunk blobs in parallel and assemble
    # them; plain files keep the single-blob Range-parallel path unchanged, so
    # every pre-chunking artifact downloads exactly as before.
    if group.is_chunked:
        if local_dir is not None:
            return _download_chunked_to_local_dir(
                group, registry, oci_repo, paths.dest_file, oci_token, manifest
            )
        return _download_chunked_to_cache(
            group=group,
            registry=registry,
            oci_repo=oci_repo,
            repo_dir=paths.repo_dir,
            snapshots_dir=paths.snapshots_dir,
            filename=filename,
            oci_token=oci_token,
            manifest=manifest,
        )
    blob_url = f"{registry}/v2/{oci_repo}/blobs/{group.digest}"
    if local_dir is not None:
        return _download_to_local_dir(blob_url, paths.dest_file, oci_token)
    return _download_to_cache(
        blob_url=blob_url,
        repo_dir=paths.repo_dir,
        snapshots_dir=paths.snapshots_dir,
        filename=filename,
        oci_token=oci_token,
        target_digest=group.digest,
    )


def _download_to_cache(
    blob_url, repo_dir, snapshots_dir, filename, oci_token, target_digest
):
    """Cache-structured download mirroring huggingface_hub's layout."""
    # Cache layout modeled on huggingface_hub
    blobs_dir = os.path.join(repo_dir, "blobs")
    os.makedirs(blobs_dir, exist_ok=True)
    os.makedirs(snapshots_dir, exist_ok=True)

    file_path = os.path.join(snapshots_dir, filename)
    # Unique per call: two concurrent downloaders writing the same logical
    # file no longer race on a shared temp path. mkstemp returns (fd, path);
    # we close the fd immediately because the Rust download_file_native
    # opens the file by path, not by inherited handle.
    safe_name = filename.replace("/", "_")
    fd, temp_path = tempfile.mkstemp(
        dir=blobs_dir,
        prefix=f"tmp_{safe_name}_",
    )
    os.close(fd)

    # 2. Concurrent download via the Rust engine
    print(f"Downloading {filename} (parallel)...")
    verify_hash = resolve_verify_hash()
    try:
        # Audit L6 (Phase 3.12): download_file_native now returns
        # Optional[str] — `None` when verify_hash=False (skipped), `str`
        # when verify_hash=True. Previously it returned `""` as an
        # in-band sentinel for "skipped", which forced callers to know
        # the convention and would collide with any legitimate empty
        # string. The dispatch below is on `is not None`, not on
        # `verify_hash`, so a future code path that returns `None` for
        # other reasons (e.g. async cancellation) routes through the
        # same fallback rather than masquerading as a valid digest.
        calculated_hash = download_file_native(
            url=blob_url,
            dest_path=temp_path,
            auth_token=oci_token,
            chunk_size=resolve_chunk_size(),
            verify_hash=verify_hash,
        )
    except Exception:
        # Clean up the mkstemp file before bubbling up. The inner OSError
        # swallow is intentional: a cleanup failure (file already gone,
        # permissions) must not shadow the original download exception.
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise

    # If hash verification was skipped (calculated_hash is None), fall
    # back to the digest from the OCI manifest. Otherwise use what the
    # Rust side computed.
    final_hash = (
        calculated_hash
        if calculated_hash is not None
        else target_digest.replace("sha256:", "")
    )

    # 3. Atomic rename of the temp file into the SHA256 blob
    blob_path = os.path.join(blobs_dir, f"sha256:{final_hash}")
    if not os.path.exists(blob_path):
        os.rename(temp_path, blob_path)
    elif os.path.exists(temp_path):
        os.remove(temp_path)

    # 4. Create the symlink in the snapshot
    _create_symlink(blob_path, file_path)
    return file_path


def _download_to_local_dir(blob_url, dest_file, oci_token):
    """Direct download to a user-chosen directory — bypasses the cache layout.

    Downloads to a per-call temp sibling and atomically `os.replace`s it into
    `dest_file` only on success. The Rust downloader pre-allocates its target
    at full size (`f.set_len`) and streams chunks into it in place, so writing
    straight to `dest_file` would leave a full-size, hole-ridden file at the
    user's path on any chunk failure or Ctrl-C — and `hf_hub_download`'s
    `os.path.exists(dest_file)` cache check would then serve that corrupt file
    as a hit on the next call. Temp-then-replace makes the user-visible path
    appear only after a fully successful download. Cleanup is on `BaseException`
    (not `Exception`) so a `KeyboardInterrupt` mid-stream also removes the
    partial temp file.
    """
    parent = os.path.dirname(dest_file)
    if parent:
        os.makedirs(parent, exist_ok=True)
    print(f"Downloading {os.path.basename(dest_file)} (parallel)...")
    # Per-call unique temp sibling in the destination directory so the final
    # `os.replace` is an atomic same-filesystem rename. mkstemp returns
    # (fd, path); close the fd because the Rust download opens the file by path.
    fd, temp_path = tempfile.mkstemp(
        dir=parent or ".",
        prefix=f".tmp_{os.path.basename(dest_file)}_",
    )
    os.close(fd)
    try:
        # local_dir mode writes to the user-chosen path and does not assemble a
        # content-addressed blob layout, so the calculated hash (Optional[str]
        # post Phase 3.12) is intentionally unused — the path is the identity.
        download_file_native(
            url=blob_url,
            dest_path=temp_path,
            auth_token=oci_token,
            chunk_size=resolve_chunk_size(),
            verify_hash=resolve_verify_hash(),
        )
    except BaseException:
        # Remove the partial temp file before propagating. The inner OSError
        # swallow mirrors `_download_to_cache`: a cleanup failure must not
        # shadow the original download exception (or KeyboardInterrupt).
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise
    os.replace(temp_path, dest_file)
    return dest_file


def _pull_packs(group, manifest, registry, oci_repo, temp_path, oci_token) -> Optional[str]:
    """Assemble a chunked-v2 file from its pack blobs.

    Fetches the pointer blob (the pack→chunk map), coalesces the chunks by pack,
    and hands the plan to the native pack assembler, which fetches each pack once
    (a full `200`) and slices its chunks to their file offsets. Whole-file digest
    is always verified — it is the only check that proves cross-pack ordering.
    """
    blob = _fetch_blob_bytes(registry, oci_repo, group.pointer_digest, oci_token)
    refs = parse_pointer_v2(blob)
    pack_layer_sizes = {
        layer["digest"]: layer["size"]
        for layer in manifest.get("layers", [])
        if layer.get("mediaType") == PACK_MEDIA_TYPE
    }
    # Group chunks by pack (first-appearance order) and compute file offsets.
    by_pack: Dict[str, list] = {}
    file_offset = 0
    for ref in refs:
        by_pack.setdefault(ref.pack_digest, []).append(
            (ref.pack_offset, ref.size, file_offset, _digest_hex(ref.chunk_digest))
        )
        file_offset += ref.size
    total_size = file_offset

    pack_urls, pack_sizes, pack_chunks = [], [], []
    for pack_digest, targets in by_pack.items():
        pack_urls.append(f"{registry}/v2/{oci_repo}/blobs/{pack_digest}")
        pack_sizes.append(pack_layer_sizes[pack_digest])
        pack_chunks.append(targets)
    return download_packs_native(
        pack_urls=pack_urls,
        pack_sizes=pack_sizes,
        pack_chunks=pack_chunks,
        dest_path=temp_path,
        total_size=total_size,
        file_digest=_digest_hex(group.digest),
        auth_token=oci_token,
        max_concurrent=resolve_max_concurrent(),
    )


def _fetch_blob_bytes(registry, oci_repo, digest, oci_token) -> bytes:
    """GET a blob's raw bytes (the v2 pointer blob is small and read whole)."""
    resp = _http.client().get(
        f"{registry}/v2/{oci_repo}/blobs/{digest}",
        headers={"Authorization": f"Bearer {oci_token}"},
        timeout=DEFAULT_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.content


def _pull_chunks(group, registry, oci_repo, temp_path, oci_token, manifest=None) -> Optional[str]:
    """Fetch a chunked-v2 file's pack blobs into `temp_path` via the Rust engine.

    Per-chunk digest verification is always on in the native layer; the whole-file
    digest is ALSO always verified — unlike the single-blob path's opt-in
    HIPPIUS_VERIFY_HASH — because per-chunk digests prove each chunk's *bytes* but
    not its *position*, and the whole-file `sha256(concat)` pass is the only check
    on ordering. Returns the computed whole-file hash.
    """
    return _pull_packs(group, manifest or {}, registry, oci_repo, temp_path, oci_token)


def _download_chunked_to_cache(
    *, group, registry, oci_repo, repo_dir, snapshots_dir, filename, oci_token, manifest=None
):
    """Chunked-file analog of `_download_to_cache`.

    Assembles the chunk blobs into a temp file, then places it in the
    content-addressed cache under the *whole-file* digest and symlinks it into
    the snapshot — so a chunked file dedups on disk against identical content
    stored plainly, and the snapshot layout is identical to the single-blob path.
    """
    blobs_dir = os.path.join(repo_dir, "blobs")
    os.makedirs(blobs_dir, exist_ok=True)
    os.makedirs(snapshots_dir, exist_ok=True)
    file_path = os.path.join(snapshots_dir, filename)

    safe_name = filename.replace("/", "_")
    fd, temp_path = tempfile.mkstemp(dir=blobs_dir, prefix=f"tmp_{safe_name}_")
    os.close(fd)

    print(f"Downloading {filename} (parallel)...")
    try:
        computed = _pull_chunks(group, registry, oci_repo, temp_path, oci_token, manifest)
    except BaseException:
        # Mirror the single-blob cleanup: drop the partial temp on any failure
        # (including KeyboardInterrupt) so a later cache-hit check can't serve it.
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise

    # When verification is skipped the native call returns None; the whole-file
    # digest from the pointer is authoritative for naming the blob either way.
    final_hash = computed if computed is not None else _digest_hex(group.digest)
    blob_path = os.path.join(blobs_dir, f"sha256:{final_hash}")
    if not os.path.exists(blob_path):
        os.rename(temp_path, blob_path)
    elif os.path.exists(temp_path):
        os.remove(temp_path)

    _create_symlink(blob_path, file_path)
    return file_path


def _download_chunked_to_local_dir(group, registry, oci_repo, dest_file, oci_token, manifest=None):
    """Chunked-file analog of `_download_to_local_dir`: assemble to a temp
    sibling, then atomically `os.replace` into `dest_file` only on full success,
    so a failed/interrupted assemble never leaves a partial file at the user path.
    """
    parent = os.path.dirname(dest_file)
    if parent:
        os.makedirs(parent, exist_ok=True)
    print(f"Downloading {os.path.basename(dest_file)} (parallel)...")
    fd, temp_path = tempfile.mkstemp(
        dir=parent or ".", prefix=f".tmp_{os.path.basename(dest_file)}_"
    )
    os.close(fd)
    try:
        _pull_chunks(group, registry, oci_repo, temp_path, oci_token, manifest)
    except BaseException:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise
    os.replace(temp_path, dest_file)
    return dest_file


def _create_symlink(src: str, dst: str) -> None:
    """Materialize the snapshot entry at `dst` pointing at the blob at `src`.

    Prefers a symlink (cheapest, allows cross-volume cache); falls back to
    a hardlink (same-volume only); falls back to a full copy (doubles disk
    use). Each fallback emits a UserWarning so operators see when the cache
    is more expensive than expected — Windows without developer mode,
    sandboxed CI without symlink capability, and removable FAT/exFAT drives
    are the common offenders.

    The previous implementation had a TOCTOU race between
    `if os.path.exists(dst): os.remove(dst)` and the subsequent
    `os.symlink(dst)`: two concurrent downloaders converging on the same
    snapshot path could observe `exists()` then race on the `remove`
    (one wins, the other gets FileNotFoundError) or both race on the
    `symlink` (one wins, the other gets FileExistsError). The fix is
    the same atomic-rename pattern used in `auth._atomic_write_secret`:
    create the new link at a sibling temp name (per-process unique),
    then `os.replace` into `dst`. Replace is atomic on POSIX same-
    filesystem renames — readers see either the old link/file or the
    new one, never an absent-during-transition state.
    """
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    tmp_dst = f"{dst}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"

    # Relative path from the snapshot directory to the blob so the cache
    # remains portable if the cache root is moved (the link target stays
    # valid as long as the relative layout is preserved).
    rel_src = os.path.relpath(src, os.path.dirname(dst))

    try:
        os.symlink(rel_src, tmp_dst)
        os.replace(tmp_dst, dst)
        return
    except OSError as e:
        # Best-effort cleanup of the tmp name if the symlink+replace
        # sequence failed before the rename landed.
        try:
            os.unlink(tmp_dst)
        except FileNotFoundError:
            pass
        warnings.warn(
            f"symlink {src!r} -> {dst!r} failed ({e}); falling back to hardlink",
            UserWarning,
            stacklevel=2,
        )

    try:
        os.link(src, tmp_dst)
        os.replace(tmp_dst, dst)
        return
    except OSError as e:
        try:
            os.unlink(tmp_dst)
        except FileNotFoundError:
            pass
        warnings.warn(
            f"hardlink {src!r} -> {dst!r} failed ({e}); falling back to full copy "
            f"-- this doubles disk usage for the snapshot",
            UserWarning,
            stacklevel=2,
        )

    # shutil.copy2 writes directly to dst (overwrites if present) and
    # is not concurrency-safe across writers — but at this point we've
    # already exhausted the link fallbacks, so we accept the residual
    # race window for the rare-path copy case. Copy to temp + replace
    # to keep behavior consistent with the link paths above.
    shutil.copy2(src, tmp_dst)
    os.replace(tmp_dst, dst)


def try_to_load_from_cache(
    repo_id: str,
    filename: str,
    cache_dir: Optional[Union[str, Path]] = None,
    revision: Optional[str] = None,
    repo_type: Optional[str] = None,
) -> Optional[str]:
    """Return the cached file path if present, or None otherwise.

    Pure local filesystem check — never hits the network. Mirrors HF's
    behavior modulo the _CACHED_NO_EXIST sentinel, which we never return
    (we don't track known-404 entries separately).
    """
    _validate_repo_type(repo_type)
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR
    if revision is None:
        revision = "main"

    file_path = os.path.join(
        str(cache_dir),
        _cache_dirname(repo_id, repo_type),
        "snapshots",
        revision,
        filename,
    )
    if os.path.exists(file_path):
        return file_path
    return None


def hf_hub_url(
    repo_id: str,
    filename: str,
    *,
    subfolder: Optional[str] = None,
    repo_type: Optional[str] = None,
    revision: Optional[str] = None,
    endpoint: Optional[str] = None,
) -> str:
    """Return an OCI manifest URL for the file.

    Unlike huggingface_hub.hf_hub_url which returns a direct CDN download
    URL, this returns the OCI manifest URL — OCI blobs are content-addressed
    and require resolving the digest from the manifest first. For actual
    downloads use hf_hub_download.
    """
    _validate_repo_type(repo_type)
    if subfolder:
        filename = f"{subfolder}/{filename}"
    base = resolve_registry(endpoint)
    rev = revision or "main"
    return f"{base}/v2/{_oci_repo_path(repo_id, repo_type)}/manifests/{rev}"


hippius_hub_download = hf_hub_download
