import os
import json
import requests
from .constants import DEFAULT_CACHE_DIR, DEFAULT_REGISTRY_URL

TOKEN_PATH = os.path.join(DEFAULT_CACHE_DIR, "token")

import base64

def login(username: str = None, password: str = None, token: str = None):
    """Save token to ~/.cache/hippius/hub/token"""
    os.makedirs(DEFAULT_CACHE_DIR, exist_ok=True)
    
    auth_str = ""
    if username and password:
        basic_auth = base64.b64encode(f"{username}:{password}".encode()).decode()
        auth_str = f"Basic {basic_auth}"
    elif token:
        auth_str = f"Bearer {token}"
    else:
        raise ValueError("Either username/password or token must be provided")
        
    with open(TOKEN_PATH, "w") as f:
        f.write(auth_str)
    print(f"Token successfully saved to {TOKEN_PATH}")

def get_token() -> str:
    """Read token if exists"""
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, "r") as f:
            return f.read().strip()
    return None

def get_docker_auth(registry_url: str) -> str:
    """Extract base64 auth string from ~/.docker/config.json for the given registry"""
    docker_config = os.path.expanduser("~/.docker/config.json")
    if not os.path.exists(docker_config):
        return None
        
    try:
        with open(docker_config, "r") as f:
            config = json.load(f)
            
        host = registry_url.replace("https://", "").replace("http://", "")
        auths = config.get("auths", {})
        
        for key, val in auths.items():
            if host in key:
                return val.get("auth")
    except Exception:
        pass
    return None

def get_oci_bearer_token(repo_id: str, token: str = None, push: bool = False) -> str:
    """Fetch OCI bearer token for the given repository using Harbor/Registry v2 API."""
    scope = f"repository:{repo_id}:pull,push" if push else f"repository:{repo_id}:pull"
    auth_url = f"{DEFAULT_REGISTRY_URL}/service/token?service=harbor-registry&scope={scope}"
    headers = {}
    
    # 1. Utilisation de ~/.docker/config.json en priorité (Basic Auth)
    if not token:
        docker_auth = get_docker_auth(DEFAULT_REGISTRY_URL)
        if docker_auth:
            headers["Authorization"] = f"Basic {docker_auth}"
            
    # 2. Fallback sur le token fourni (souvent utilisé comme Bearer ou Basic selon configuration Harbor)
    if not headers.get("Authorization") and token:
        if token.startswith("Basic ") or token.startswith("Bearer "):
            headers["Authorization"] = token
        else:
            # Backward compatibility
            headers["Authorization"] = f"Bearer {token}"
    
    resp = requests.get(auth_url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("token")
