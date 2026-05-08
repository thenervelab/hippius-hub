import os
import shutil
from typing import Optional
from .auth import get_token, get_oci_bearer_token
from .constants import DEFAULT_CACHE_DIR, DEFAULT_REGISTRY_URL

try:
    from .hippius_core import download_file_native
except ImportError:
    raise ImportError("hippius_core is not installed. Did you run `maturin develop`?")

def hippius_hub_download(
    repo_id: str,
    filename: str,
    revision: Optional[str] = "main",
    cache_dir: Optional[str] = None,
    token: Optional[str] = None,
    chunk_size: Optional[int] = 100 * 1024 * 1024,
    verify_hash: bool = False,
) -> str:
    """
    Drop-in replacement for hf_hub_download.
    Downloads a file via Hippius OCI Registry using a fast concurrent Rust engine.
    verify_hash is False by default to maximize assembly speed, matching HF behavior.
    """
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR
        
    if token is None:
        token = get_token()
        
    oci_token = get_oci_bearer_token(repo_id, token)
    
    import requests
    # Récupération du manifest OCI pour trouver le digest exact du fichier
    manifest_url = f"{DEFAULT_REGISTRY_URL}/v2/{repo_id}/manifests/{revision}"
    headers = {"Authorization": f"Bearer {oci_token}", "Accept": "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"}
    
    resp = requests.get(manifest_url, headers=headers)
    if resp.status_code == 404:
        raise ValueError(f"Revision '{revision}' not found in repository '{repo_id}'")
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
        raise ValueError(f"File '{filename}' not found in the OCI manifest of '{repo_id}:{revision}'")
        
    url = f"{DEFAULT_REGISTRY_URL}/v2/{repo_id}/blobs/{target_digest}"
    
    # Structure de cache calquée sur huggingface_hub
    repo_dir = os.path.join(cache_dir, f"models--{repo_id.replace('/', '--')}")
    blobs_dir = os.path.join(repo_dir, "blobs")
    snapshots_dir = os.path.join(repo_dir, "snapshots", revision)
    
    os.makedirs(blobs_dir, exist_ok=True)
    os.makedirs(snapshots_dir, exist_ok=True)
    
    file_path = os.path.join(snapshots_dir, filename)
    
    # 1. Vérification du cache : ne jamais retélécharger un fichier existant
    if os.path.exists(file_path):
        return file_path
        
    # 2. Téléchargement concurrent via le moteur Rust
    temp_path = os.path.join(blobs_dir, f"tmp_{filename.replace('/', '_')}")
    
    print(f"Téléchargement concurrent de {filename} via hippius_core (Rust)...")
    calculated_hash = download_file_native(
        url=url, 
        dest_path=temp_path, 
        auth_token=oci_token, 
        chunk_size=chunk_size,
        verify_hash=verify_hash
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
