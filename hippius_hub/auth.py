import os
import json
import threading
import time
from typing import Literal, Union
import httpx
from .constants import DEFAULT_CACHE_DIR, DEFAULT_HTTP_TIMEOUT, DEFAULT_REGISTRY_URL
from .errors import LocalTokenNotFoundError

TOKEN_PATH = os.path.join(DEFAULT_CACHE_DIR, "token")

import base64


# Per-(repo_id, push, auth_input) cache of OCI bearer JWTs.
# Lock guards both insertion and eviction; reads of expired entries fall back
# to a fresh fetch, so it's OK that the lookup-then-update is non-atomic.
_OCI_TOKEN_CACHE = {}
_OCI_TOKEN_CACHE_LOCK = threading.Lock()
_OCI_TOKEN_LEEWAY_SECONDS = 30  # refresh tokens 30s before they actually expire


def _jwt_expiration(jwt_str: str):
    """Return the `exp` claim (Unix ts) of a JWT, or None if it can't be parsed."""
    parts = jwt_str.split(".")
    if len(parts) != 3:
        return None
    payload_b64 = parts[1]
    padding = "=" * (-len(payload_b64) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload_b64 + padding)
        payload = json.loads(decoded)
    except (ValueError, json.JSONDecodeError):
        return None
    return payload.get("exp")


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

    # Write+chmod together so the file is never world-readable mid-flight.
    # os.chmod on a path is a syscall, not enforced atomically with open();
    # the best-effort try/except mirrors save_api_token in console.py.
    with open(TOKEN_PATH, "w") as f:
        f.write(auth_str)
    try:
        os.chmod(TOKEN_PATH, 0o600)
    except OSError:
        pass
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


def resolve_token_value(token):
    """Translate HF's three-state token argument (None/True=saved, False=no auth,
    str=use literal) into the raw token / pre-wrapped header value used downstream
    by `get_oci_bearer_token` (which accepts either form).

    `False` is forwarded verbatim as the HF "anonymous; do not auto-discover"
    sentinel — downstream callers (notably `get_oci_bearer_token`) distinguish
    "no preference (None)" from "explicit no-auth (False)" and only the former
    consults the docker-config fallback. Collapsing False to None here would
    silently push under the user's docker credentials.
    """
    if token is False:
        return False  # propagate the anonymous sentinel
    if isinstance(token, str):
        return token
    return get_token()


def resolve_auth_header(token):
    """Same three-state input, but always returns a full Authorization header
    (`Bearer ...` / `Basic ...`) or None. Use for direct Harbor admin-API calls
    which need a complete header string.

    `resolve_token_value` now forwards `False` as the HF anonymous sentinel
    (rather than collapsing to `None`), so we treat both `False` and `None`
    as "no header" here — the admin-API behavior is unchanged.
    """
    value = resolve_token_value(token)
    if value is None or value is False:
        return None
    if value.startswith(("Basic ", "Bearer ")):
        return value
    return f"Bearer {value}"


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

        host = registry_url.replace("https://", "").replace("http://", "").rstrip("/")
        auths = config.get("auths", {})

        # Match on host, not substring — otherwise "registry.hippius.com" would
        # match "registry.hippius.com.evil.example", a classic confused-deputy.
        for key, val in auths.items():
            key_host = key.replace("https://", "").replace("http://", "").rstrip("/")
            if key_host == host:
                return val.get("auth")
    except Exception:
        pass
    return None

def get_oci_bearer_token(
    repo_id: str,
    token: Union[str, Literal[False], None] = None,
    push: bool = False,
    use_cache: bool = True,
) -> str:
    """Fetch an OCI bearer token from the Hippius registry token endpoint.

    Per-`(repo_id, push, auth_input)` cache, with TTL parsed from the JWT's
    own `exp` claim minus a 30s leeway. Pass `use_cache=False` to bypass.
    Reduces token-service round-trips when callers chain multiple ops on
    the same repo (e.g. `repo_exists()` then `revision_exists()`).

    `token` accepts the HF three-state convention: a string (use it directly),
    `None` (no caller preference — try docker-config fallback), or `False`
    (HF's anonymous sentinel — caller explicitly opted out of auth, so do not
    consult any ambient credential source). The `False` case is load-bearing
    for security: a caller asking for anonymous I/O must not be silently
    elevated to the user's docker-stored creds.
    """
    cache_key = (repo_id, push, token)
    now = time.time()

    if use_cache:
        with _OCI_TOKEN_CACHE_LOCK:
            cached = _OCI_TOKEN_CACHE.get(cache_key)
        if cached is not None:
            cached_token, cached_exp = cached
            if cached_exp - _OCI_TOKEN_LEEWAY_SECONDS > now:
                return cached_token

    scope = f"repository:{repo_id}:pull,push" if push else f"repository:{repo_id}:pull"
    auth_url = f"{DEFAULT_REGISTRY_URL}/service/token?service=harbor-registry&scope={scope}"
    headers = {}

    # `token is False` is the HF sentinel for "anonymous; do not auto-discover".
    # We must distinguish it from `None` ("no preference"), because only the
    # latter is allowed to fall back to ambient docker credentials.
    no_auth = token is False
    effective_token = None if no_auth else token

    # 1. Prefer ~/.docker/config.json if present (Basic Auth), but only when the
    #    caller hasn't explicitly opted out via token=False.
    if not effective_token and not no_auth:
        docker_auth = get_docker_auth(DEFAULT_REGISTRY_URL)
        if docker_auth:
            headers["Authorization"] = f"Basic {docker_auth}"

    # 2. Fall back to the provided token (Bearer or Basic depending on registry config)
    if not headers.get("Authorization") and effective_token:
        if effective_token.startswith(("Basic ", "Bearer ")):
            headers["Authorization"] = effective_token
        else:
            # Backward compatibility
            headers["Authorization"] = f"Bearer {effective_token}"

    resp = httpx.get(auth_url, headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
    resp.raise_for_status()
    fresh_token = resp.json().get("token")

    if use_cache and fresh_token:
        exp = _jwt_expiration(fresh_token)
        if exp is not None:
            with _OCI_TOKEN_CACHE_LOCK:
                _OCI_TOKEN_CACHE[cache_key] = (fresh_token, exp)

    return fresh_token


def clear_oci_token_cache():
    """Drop all cached OCI bearer tokens. For tests that monkeypatch credentials."""
    with _OCI_TOKEN_CACHE_LOCK:
        _OCI_TOKEN_CACHE.clear()
