import os
import json
import httpx
from .constants import DEFAULT_CACHE_DIR, DEFAULT_REGISTRY_URL
from .errors import LocalTokenNotFoundError

TOKEN_PATH = os.path.join(DEFAULT_CACHE_DIR, "token")

import base64


def login(
    token: str = None,
    *,
    username: str = None,
    password: str = None,
    add_to_git_credential: bool = False,
    skip_if_logged_in: bool = True,
):
    """Save credentials to ~/.cache/hippius/hub/token.

    Accepts both the huggingface_hub shape — `login(token=...)` — and our
    Harbor-specific Basic-auth shape — `login(username=..., password=...)`.
    `add_to_git_credential` and `skip_if_logged_in` are accepted for HF
    signature compatibility and currently have no effect (Hippius doesn't
    use git credential storage).
    """
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


def logout(token_name: str = None):
    """Remove the saved token file. `token_name` is accepted for HF parity
    but ignored (we only store one set of credentials)."""
    if os.path.exists(TOKEN_PATH):
        os.remove(TOKEN_PATH)


def get_token() -> str:
    """Read token if exists"""
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, "r") as f:
            return f.read().strip()
    return None


def whoami(token=None, *, endpoint: str = None) -> dict:
    """Return user info shaped like huggingface_hub.whoami.

    `token` follows HF semantics: None/True use the saved token, False raises,
    a string is used directly (auto-wrapped as `Bearer <str>` if it isn't already
    a full `Basic ...` / `Bearer ...` header).
    """
    from ._harbor import harbor_whoami

    if token is False:
        raise LocalTokenNotFoundError("token=False but whoami requires authentication")
    if token is None or token is True:
        auth_header = get_token()
        if not auth_header:
            raise LocalTokenNotFoundError(
                "No saved token found; run `hippius-hub login` first."
            )
    elif isinstance(token, str):
        if token.startswith(("Basic ", "Bearer ")):
            auth_header = token
        else:
            auth_header = f"Bearer {token}"
    else:
        raise TypeError(f"Unsupported token type: {type(token).__name__}")
    return harbor_whoami(auth_header, endpoint=endpoint)


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
    """Fetch an OCI bearer token from the Hippius registry token endpoint."""
    scope = f"repository:{repo_id}:pull,push" if push else f"repository:{repo_id}:pull"
    auth_url = f"{DEFAULT_REGISTRY_URL}/service/token?service=harbor-registry&scope={scope}"
    headers = {}

    # 1. Utilisation de ~/.docker/config.json en priorité (Basic Auth)
    if not token:
        docker_auth = get_docker_auth(DEFAULT_REGISTRY_URL)
        if docker_auth:
            headers["Authorization"] = f"Basic {docker_auth}"

    # 2. Fallback sur le token fourni (Bearer ou Basic selon la config registry)
    if not headers.get("Authorization") and token:
        if token.startswith("Basic ") or token.startswith("Bearer "):
            headers["Authorization"] = token
        else:
            # Backward compatibility
            headers["Authorization"] = f"Bearer {token}"

    resp = httpx.get(auth_url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("token")
