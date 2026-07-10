"""Token and credential plumbing for the OCI registry.

Resolves HF's three-state `token` argument (None/True/False/str) into either
a saved-on-disk token or a literal, caches short-lived OCI bearer tokens per
(repo, scope) so each download/upload doesn't re-auth, and exposes the
saved-token persistence used by `login` / `logout` / `whoami`.
"""
import base64
import hashlib
import json
import os
import tempfile
import threading
import time
import warnings
from typing import Literal, Optional, Union

from . import _http
from .constants import (
    DEFAULT_CACHE_DIR,
    DEFAULT_HTTP_TIMEOUT,
    DEFAULT_REGISTRY_URL,
    resolve_registry,
)
from .errors import LocalTokenNotFoundError

TOKEN_PATH = os.path.join(DEFAULT_CACHE_DIR, "token")


# Per-(repo_id, push, auth_input) cache of OCI bearer JWTs.
# Lock guards both insertion and eviction; reads of expired entries fall back
# to a fresh fetch, so it's OK that the lookup-then-update is non-atomic.
_OCI_TOKEN_CACHE = {}
_OCI_TOKEN_CACHE_LOCK = threading.Lock()
_OCI_TOKEN_LEEWAY_SECONDS = 30  # refresh tokens 30s before they actually expire


def _token_cache_key(repo_id: str, push: bool, token, registry: str) -> tuple:
    """Cache key for OCI bearer tokens — hashes the token so plaintext
    credentials never appear in the in-memory _OCI_TOKEN_CACHE dict.
    Future debug dumps of the cache won't leak the secret.

    `registry` is part of the key (INPUT-1): tokens are minted per-origin, so a
    token issued by the default registry must never be served from cache for a
    request aimed at a different `endpoint` — that would forward a
    default-origin credential off-origin.
    """
    if token is False:
        return (repo_id, push, "<anon>", registry)
    if not token:
        return (repo_id, push, None, registry)
    return (repo_id, push, hashlib.sha256(token.encode()).hexdigest(), registry)


def _forbid_offorigin_ambient_credentials(token, endpoint) -> None:
    """Refuse to forward a stored/ambient credential to a non-default registry.

    INPUT-1 confused-deputy guard. A credential read from the saved login file
    (`token` is `None`/`True`) was entered for the default Hippius registry;
    forwarding it to a caller-supplied `endpoint` would leak it to that origin.
    Only the saved-login credential is gated — an explicit literal `token` is
    the caller's own credential (they chose to send it), and the docker-config
    fallback is already host-scoped by `get_docker_auth`. Anonymous use of a
    custom endpoint (no saved credential present) is left untouched.
    """
    if token is not None and token is not True:
        return  # explicit literal, or False (anonymous) — caller's own choice
    if resolve_registry(endpoint) == DEFAULT_REGISTRY_URL:
        return  # same origin the saved credential was entered for
    if get_token() is None:
        return  # nothing stored to leak — allow anonymous access to the endpoint
    raise ValueError(
        "Refusing to send your stored login credentials to a non-default "
        f"registry endpoint ({resolve_registry(endpoint)!r}). Pass an explicit "
        "token=... for custom endpoints so ambient credentials are not "
        "forwarded off-origin."
    )


def _jwt_expiration(jwt_str: str):
    """Return the `exp` claim (Unix ts) of a JWT, or None if it can't be parsed.

    Surfaces parse failures via UserWarning rather than returning None silently
    — a malformed JWT means the token will never be cached, which manifests as
    a silent perf regression (every call re-fetches from the token endpoint).
    """
    parts = jwt_str.split(".")
    if len(parts) != 3:
        warnings.warn(
            "JWT does not have 3 segments; cannot extract exp claim (token will not be cached)",
            UserWarning,
            stacklevel=2,
        )
        return None
    payload_b64 = parts[1]
    padding = "=" * (-len(payload_b64) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload_b64 + padding)
        payload = json.loads(decoded)
    except (ValueError, json.JSONDecodeError) as e:
        # ValueError covers: binascii.Error (bad b64), UnicodeDecodeError
        # (json.loads on non-UTF-8 bytes), and JSONDecodeError. All three
        # share that ancestor in modern CPython.
        warnings.warn(
            f"JWT payload parse failed ({e}); token will not be cached",
            UserWarning,
            stacklevel=2,
        )
        return None
    # A JWT payload MUST be a JSON object per RFC 7519 §4. A pathological
    # token containing `null`, `42`, or `[]` as its middle segment decodes
    # to a non-dict — calling `.get("exp")` on that would raise an
    # AttributeError that the caller doesn't expect from a parse helper.
    if not isinstance(payload, dict):
        warnings.warn(
            f"JWT payload is not a JSON object ({type(payload).__name__}); "
            "token will not be cached",
            UserWarning,
            stacklevel=2,
        )
        return None
    return payload.get("exp")


def _atomic_write_secret(path: str, content: str) -> None:
    """Write `content` to `path` such that the file is NEVER world-readable.

    The previous implementation (write-then-chmod) opened the file with
    the process umask — typically 0o022 on Linux, leaving the file at
    0o644 until the subsequent ``os.chmod`` call landed. A reader racing
    that window saw the bearer token. Even on a sub-millisecond window
    that's a real exfiltration vector on shared hosts (CI runners,
    multi-tenant laptops).

    Pattern: write to a sibling tempfile (mkstemp creates with 0o600 on
    POSIX), then ``os.rename`` over the destination. Rename is atomic on
    POSIX same-filesystem moves — readers either see the old file or
    the new file, never a transient half-written or wrong-mode state.

    On Windows, ``os.chmod`` is a near-no-op and mkstemp's mode is best-
    effort; the pattern still works because Windows ACLs default to
    owner-only on tempfile creation.
    """
    parent = os.path.dirname(path) or "."
    fd, tmp_path = tempfile.mkstemp(dir=parent, prefix=".token-", suffix=".tmp")
    try:
        # mkstemp returns 0o600 on POSIX but enforce explicitly so a
        # future stdlib change (or a non-standard tempfile impl) cannot
        # downgrade us silently.
        try:
            os.fchmod(fd, 0o600)
        except (AttributeError, OSError):
            # os.fchmod doesn't exist on Windows; mkstemp's default is
            # already owner-only on that platform.
            pass
        with os.fdopen(fd, "w") as f:
            f.write(content)
        # os.replace == os.rename with overwrite semantics on POSIX,
        # better Windows behavior. Atomic same-filesystem rename:
        # readers always see EITHER the old path or the new — never an
        # in-flight wrong-mode file.
        os.replace(tmp_path, path)
    except BaseException:
        # mkstemp left a tempfile on disk; remove it if we didn't
        # successfully rename. The bare except is deliberate — any
        # path that didn't reach os.replace must clean up, including
        # KeyboardInterrupt/SystemExit.
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


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

    The token file is written via an atomic-rename pattern (see
    `_atomic_write_secret`) so it is never observable at any mode other
    than 0o600 — the previous write-then-chmod pattern left a transient
    world-readable window between open() and chmod().
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

    _atomic_write_secret(TOKEN_PATH, auth_str)
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

    Internally parses through `_token.TokenInput` so the three semantic cases
    are dispatched on a typed tagged union rather than scattered isinstance /
    `is False` checks — the public contract (input shape + return values) is
    unchanged.
    """
    from ._token import from_hf, Anonymous, Literal as TokenLiteral

    parsed = from_hf(token)
    if isinstance(parsed, Anonymous):
        return False  # propagate the anonymous sentinel
    if isinstance(parsed, TokenLiteral):
        return parsed.value
    # UseStored
    return get_token()


def resolve_auth_header(token, *, endpoint: Optional[str] = None):
    """Same three-state input, but always returns a full Authorization header
    (`Bearer ...` / `Basic ...`) or None. Use for direct Harbor admin-API calls
    which need a complete header string.

    `resolve_token_value` now forwards `False` as the HF anonymous sentinel
    (rather than collapsing to `None`), so we treat both `False` and `None`
    as "no header" here — the admin-API behavior is unchanged.

    `endpoint` is the credential trust boundary (INPUT-1): a stored login
    credential is never returned for a non-default endpoint, so a Harbor admin
    call against a caller-supplied registry can't leak the saved Basic
    `user:password` off-origin.
    """
    value = resolve_token_value(token)
    if value is None or value is False:
        return None
    _forbid_offorigin_ambient_credentials(token, endpoint)
    if value.startswith(("Basic ", "Bearer ")):
        return value
    return f"Bearer {value}"


def whoami(token=None, *, endpoint: str = None) -> dict:
    """Return user info shaped like huggingface_hub.whoami.

    `token` follows HF semantics: None/True use the saved token, False raises,
    a string is used directly (auto-wrapped as `Bearer <str>` if it isn't already
    a full `Basic ...` / `Bearer ...` header).

    Dispatch goes through `_token.TokenInput` so each branch corresponds to a
    distinct dataclass variant — `from_hf` rejects unsupported types at the
    boundary, removing the trailing `else: raise TypeError` that used to live
    inline here.
    """
    from ._harbor import harbor_whoami
    from ._token import from_hf, Anonymous, UseStored, Literal as TokenLiteral

    parsed = from_hf(token)
    if isinstance(parsed, Anonymous):
        raise LocalTokenNotFoundError("token=False but whoami requires authentication")
    if isinstance(parsed, UseStored):
        # Don't forward the saved credential to a caller-supplied endpoint.
        _forbid_offorigin_ambient_credentials(token, endpoint)
        auth_header = get_token()
        if not auth_header:
            raise LocalTokenNotFoundError(
                "No saved token found; run `hippius-hub login` first."
            )
    else:
        # TokenLiteral — narrowed by from_hf; mypy/ty can prove this is the only
        # remaining variant given Anonymous and UseStored are handled above.
        assert isinstance(parsed, TokenLiteral)
        if parsed.value.startswith(("Basic ", "Bearer ")):
            auth_header = parsed.value
        else:
            auth_header = f"Bearer {parsed.value}"
    return harbor_whoami(auth_header, endpoint=endpoint)


def get_docker_auth(registry_url: str) -> Optional[str]:
    """Extract base64 auth string from ~/.docker/config.json for the given registry.

    Returns None when the config doesn't exist or doesn't have a matching entry.
    Emits a UserWarning (and still returns None) when the file exists but
    can't be read/parsed — that case often means the user thinks they're
    logged in but isn't, and silently falling through to a 401 wastes
    debugging time.
    """
    docker_config = os.path.expanduser("~/.docker/config.json")
    if not os.path.exists(docker_config):
        return None
    try:
        with open(docker_config, "r") as f:
            config = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        warnings.warn(
            f"docker config at {docker_config} is unreadable ({e}); "
            "treating as no creds.",
            UserWarning,
            stacklevel=2,
        )
        return None
    host = registry_url.replace("https://", "").replace("http://", "").rstrip("/")
    # Match on host, not substring — otherwise "registry.hippius.com" would
    # match "registry.hippius.com.evil.example", a classic confused-deputy.
    for key, val in config.get("auths", {}).items():
        key_host = key.replace("https://", "").replace("http://", "").rstrip("/")
        if key_host == host:
            return val.get("auth")
    return None

def get_oci_bearer_token(
    repo_id: str,
    token: Union[str, Literal[False], None] = None,
    push: bool = False,
    use_cache: bool = True,
    endpoint: Optional[str] = None,
) -> str:
    """Fetch an OCI bearer token from the registry's token endpoint.

    Per-`(repo_id, push, auth_input, registry)` cache, with TTL parsed from the
    JWT's own `exp` claim minus a 30s leeway. Pass `use_cache=False` to bypass.
    Reduces token-service round-trips when callers chain multiple ops on the
    same repo (e.g. `repo_exists()` then `revision_exists()`).

    `token` accepts the HF three-state convention: a string (use it directly),
    `None` (no caller preference — try docker-config fallback), or `False`
    (HF's anonymous sentinel — caller explicitly opted out of auth, so do not
    consult any ambient credential source). The `False` case is load-bearing
    for security: a caller asking for anonymous I/O must not be silently
    elevated to the user's docker-stored creds.

    `endpoint` is a credential trust boundary (INPUT-1). The token is minted
    from `resolve_registry(endpoint)` — the same origin it is sent to — never
    from a hard-coded default, so a Hippius-issued token can't be harvested by
    pointing the client at an attacker endpoint. The docker-config fallback is
    looked up for that same origin (host-scoped), and a stored login credential
    is refused for any non-default endpoint via the off-origin guard.
    """
    registry = resolve_registry(endpoint)
    # Refuse to forward a saved login credential to a non-default origin before
    # resolving it into a header (confused-deputy guard).
    _forbid_offorigin_ambient_credentials(token, endpoint)
    auth_input = resolve_token_value(token)

    cache_key = _token_cache_key(repo_id, push, auth_input, registry)
    now = time.time()

    if use_cache:
        with _OCI_TOKEN_CACHE_LOCK:
            cached = _OCI_TOKEN_CACHE.get(cache_key)
        if cached is not None:
            cached_token, cached_exp = cached
            if cached_exp - _OCI_TOKEN_LEEWAY_SECONDS > now:
                return cached_token

    scope = f"repository:{repo_id}:pull,push" if push else f"repository:{repo_id}:pull"
    auth_url = f"{registry}/service/token?service=harbor-registry&scope={scope}"
    headers = {}

    # `auth_input is False` is the HF sentinel for "anonymous; do not
    # auto-discover". We must distinguish it from `None` ("no preference"),
    # because only the latter is allowed to fall back to ambient docker creds.
    no_auth = auth_input is False
    effective_token = None if no_auth else auth_input

    # 1. Prefer ~/.docker/config.json for THIS registry (Basic Auth), but only
    #    when the caller hasn't explicitly opted out via token=False. The lookup
    #    is host-scoped, so a custom endpoint only matches creds the user
    #    actually stored for that endpoint's host.
    if not effective_token and not no_auth:
        docker_auth = get_docker_auth(registry)
        if docker_auth:
            headers["Authorization"] = f"Basic {docker_auth}"

    # 2. Fall back to the provided token (Bearer or Basic depending on registry config)
    if not headers.get("Authorization") and effective_token:
        if effective_token.startswith(("Basic ", "Bearer ")):
            headers["Authorization"] = effective_token
        else:
            # Backward compatibility
            headers["Authorization"] = f"Bearer {effective_token}"

    resp = _http.client().get(auth_url, headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
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
