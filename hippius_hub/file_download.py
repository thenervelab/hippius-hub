import os
import shutil
from pathlib import Path
from typing import Dict, Optional, Union

from .auth import get_token, get_oci_bearer_token
from .constants import DEFAULT_CACHE_DIR, DEFAULT_REGISTRY_URL
from .errors import (
    EntryNotFoundError,
    LocalEntryNotFoundError,
    RevisionNotFoundError,
)

try:
    from .hippius_core import download_file_native
except ImportError:
    raise ImportError("hippius_core is not installed. Did you run `maturin develop`?")


_VALID_REPO_TYPES = (None, "model", "models")
_DEFAULT_CHUNK_SIZE = 100 * 1024 * 1024


def _resolve_chunk_size() -> int:
    raw = os.environ.get("HIPPIUS_CHUNK_SIZE")
    return int(raw) if raw else _DEFAULT_CHUNK_SIZE


def _resolve_verify_hash() -> bool:
    raw = os.environ.get("HIPPIUS_VERIFY_HASH", "").lower()
    return raw in ("1", "true", "yes")


def _resolve_auth_token(token: Union[bool, str, None]) -> Optional[str]:
    """Resolve HF-style token argument: False=no auth, str=use literal, None/True=saved."""
    if token is False:
        return None
    if isinstance(token, str):
        return token
    return get_token()


def _validate_repo_type(repo_type: Optional[str]):
    if repo_type not in _VALID_REPO_TYPES:
        raise NotImplementedError(
            f"repo_type={repo_type!r} is not supported by hippius_hub. "
            "Only model repositories are supported in this version."
        )


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
) -> str:
    """Drop-in replacement for huggingface_hub.hf_hub_download against an
    OCI-backed Hippius registry.

    Honored kwargs: subfolder, revision, cache_dir, local_dir, force_download,
    token, local_files_only, endpoint, repo_type (model-only).
    Accepted but currently ignored: etag_timeout, tqdm_class, dry_run, headers.

    hippius_hub-specific overrides via env: HIPPIUS_CHUNK_SIZE, HIPPIUS_VERIFY_HASH.
    """
    _validate_repo_type(repo_type)
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR
    cache_dir = str(cache_dir)
    if revision is None:
        revision = "main"
    if subfolder:
        filename = f"{subfolder}/{filename}"

    if local_dir is not None:
        dest_file = os.path.join(str(local_dir), filename)
    else:
        repo_dir = os.path.join(cache_dir, f"models--{repo_id.replace('/', '--')}")
        snapshots_dir = os.path.join(repo_dir, "snapshots", revision)
        dest_file = os.path.join(snapshots_dir, filename)

    # 1. Vérification du cache : ne jamais retélécharger un fichier existant
    if not force_download and os.path.exists(dest_file):
        return dest_file

    if local_files_only:
        raise LocalEntryNotFoundError(
            f"{filename!r} not found in local cache (cache_dir={cache_dir!r}) "
            f"and local_files_only=True"
        )

    registry = (endpoint or DEFAULT_REGISTRY_URL).rstrip("/")
    auth_token = _resolve_auth_token(token)
    oci_token = get_oci_bearer_token(repo_id, auth_token)

    import httpx
    # Récupération du manifest OCI pour trouver le digest exact du fichier
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
    layers = manifest.get("layers", [])
    target_digest = None

    for layer in layers:
        annotations = layer.get("annotations", {})
        title = annotations.get("org.opencontainers.image.title")
        if title == filename:
            target_digest = layer.get("digest")
            break

    if not target_digest:
        raise EntryNotFoundError(
            f"File '{filename}' not found in the OCI manifest of '{repo_id}:{revision}'"
        )

    blob_url = f"{registry}/v2/{repo_id}/blobs/{target_digest}"

    if local_dir is not None:
        return _download_to_local_dir(blob_url, dest_file, oci_token)

    return _download_to_cache(
        blob_url=blob_url,
        repo_dir=repo_dir,
        snapshots_dir=snapshots_dir,
        filename=filename,
        oci_token=oci_token,
        target_digest=target_digest,
    )


def _download_to_cache(blob_url, repo_dir, snapshots_dir, filename, oci_token, target_digest):
    """Cache-structured download mirroring huggingface_hub's layout."""
    # Structure de cache calquée sur huggingface_hub
    blobs_dir = os.path.join(repo_dir, "blobs")
    os.makedirs(blobs_dir, exist_ok=True)
    os.makedirs(snapshots_dir, exist_ok=True)

    file_path = os.path.join(snapshots_dir, filename)
    temp_path = os.path.join(blobs_dir, f"tmp_{filename.replace('/', '_')}")

    # 2. Téléchargement concurrent via le moteur Rust
    print(f"Téléchargement concurrent de {filename}...")
    verify_hash = _resolve_verify_hash()
    calculated_hash = download_file_native(
        url=blob_url,
        dest_path=temp_path,
        auth_token=oci_token,
        chunk_size=_resolve_chunk_size(),
        verify_hash=verify_hash,
    )

    # Si on ne vérifie pas le hash, on utilise le digest connu du manifeste OCI
    final_hash = calculated_hash if verify_hash else target_digest.replace("sha256:", "")

    # 3. Renommage atomique du fichier temporaire vers le blob SHA256
    blob_path = os.path.join(blobs_dir, f"sha256:{final_hash}")
    if not os.path.exists(blob_path):
        os.rename(temp_path, blob_path)
    elif os.path.exists(temp_path):
        os.remove(temp_path)

    # 4. Création du symlink dans le snapshot
    _create_symlink(blob_path, file_path)
    return file_path


def _download_to_local_dir(blob_url, dest_file, oci_token):
    """Direct download to a user-chosen directory — bypasses the cache layout."""
    parent = os.path.dirname(dest_file)
    if parent:
        os.makedirs(parent, exist_ok=True)
    print(f"Téléchargement concurrent de {os.path.basename(dest_file)}...")
    download_file_native(
        url=blob_url,
        dest_path=dest_file,
        auth_token=oci_token,
        chunk_size=_resolve_chunk_size(),
        verify_hash=_resolve_verify_hash(),
    )
    return dest_file


def _create_symlink(src: str, dst: str):
    """Crée un lien symbolique avec fallback silencieux pour Windows ou systèmes restreints."""
    if os.path.exists(dst):
        os.remove(dst)

    os.makedirs(os.path.dirname(dst), exist_ok=True)

    try:
        # Chemin relatif depuis le dossier du snapshot vers le blob
        rel_src = os.path.relpath(src, os.path.dirname(dst))
        os.symlink(rel_src, dst)
    except OSError:
        # Fallback 1: Hardlink
        try:
            os.link(src, dst)
        except OSError:
            # Fallback 2: Copie standard complète (silencieuse)
            shutil.copy2(src, dst)


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
        f"models--{repo_id.replace('/', '--')}",
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
    base = (endpoint or DEFAULT_REGISTRY_URL).rstrip("/")
    rev = revision or "main"
    return f"{base}/v2/{repo_id}/manifests/{rev}"


hippius_hub_download = hf_hub_download
