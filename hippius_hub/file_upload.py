import os
import requests
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from .auth import get_token, get_oci_bearer_token
from .constants import DEFAULT_REGISTRY_URL

try:
    from .hippius_core import hash_file_native, upload_blob_native
except ImportError:
    raise ImportError("hippius_core is not installed. Did you run `maturin develop`?")

def hippius_hub_upload(
    repo_id: str,
    local_path: str,
    revision: Optional[str] = "main",
    token: Optional[str] = None,
):
    """
    Uploads a directory or a single file to the OCI registry.
    Each file becomes a distinct layer in the OCI manifest.
    """
    if token is None:
        token = get_token()
        
    oci_token = get_oci_bearer_token(repo_id, token, push=True)
    
    files_to_upload = []
    
    if os.path.isfile(local_path):
        files_to_upload.append(local_path)
        base_dir = os.path.dirname(os.path.abspath(local_path))
    else:
        base_dir = os.path.abspath(local_path)
        for root, _, files in os.walk(base_dir):
            for file in files:
                files_to_upload.append(os.path.join(root, file))

    if not files_to_upload:
        print("❌ No files found to upload.")
        return

    print(f"📦 Preparing to upload {len(files_to_upload)} file(s) to {repo_id}:{revision}...")

    layers = []
    
    def process_file(file_path):
        rel_path = os.path.relpath(file_path, base_dir)
        # 1. Hashing local file (fast Rust)
        sha256_hash, file_size = hash_file_native(file_path)
        digest = f"sha256:{sha256_hash}"
        
        # 2. Check if blob already exists
        check_url = f"{DEFAULT_REGISTRY_URL}/v2/{repo_id}/blobs/{digest}"
        headers = {"Authorization": f"Bearer {oci_token}"}
        resp_check = requests.head(check_url, headers=headers)
        
        if resp_check.status_code == 200:
            print(f"✅ Cache HIT (skipped): {rel_path}")
        else:
            print(f"🚀 Uploading: {rel_path} ({file_size} bytes)...")
            # 3. Initiate upload
            init_url = f"{DEFAULT_REGISTRY_URL}/v2/{repo_id}/blobs/uploads/"
            resp_init = requests.post(init_url, headers=headers)
            resp_init.raise_for_status()
            
            location = resp_init.headers.get("Location")
            if not location:
                raise ValueError("No Location header returned by Harbor for upload initiation")
                
            # If location is relative, make it absolute
            if location.startswith("/"):
                location = f"{DEFAULT_REGISTRY_URL}{location}"
                
            # 4. Stream upload via Rust
            separator = "&" if "?" in location else "?"
            upload_url = f"{location}{separator}digest={digest}"
            
            upload_blob_native(upload_url, file_path, oci_token)
            print(f"✅ Uploaded: {rel_path}")
            
        return {
            "mediaType": "application/octet-stream",
            "size": file_size,
            "digest": digest,
            "annotations": {
                "org.opencontainers.image.title": rel_path.replace("\\", "/")
            }
        }

    # Parallelize file checks and uploads
    with ThreadPoolExecutor(max_workers=8) as executor:
        layers = list(executor.map(process_file, files_to_upload))

    # Build and Push OCI Manifest
    manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            "size": 2,
            "data": "e30="
        },
        "layers": layers,
        "annotations": {
            "org.opencontainers.image.source": "hippius-hub"
        }
    }

    manifest_url = f"{DEFAULT_REGISTRY_URL}/v2/{repo_id}/manifests/{revision}"
    headers_manifest = {
        "Authorization": f"Bearer {oci_token}",
        "Content-Type": "application/vnd.oci.image.manifest.v1+json"
    }
    
    print(f"📝 Publishing OCI Manifest for {revision}...")
    resp_manifest = requests.put(manifest_url, headers=headers_manifest, json=manifest)
    resp_manifest.raise_for_status()
    
    print(f"🎉 Successfully pushed {len(layers)} file(s) to {repo_id}:{revision}")
