"""Client for the Hippius backend API.

The user gets a personal API token from console.hippius.com. With that
token the CLI can:
  - list pricing plans
  - check namespace availability
  - provision their registry namespace (which gives them a docker login)
  - list their repos, artifacts, usage, audit events
  - rotate the robot secret used by `docker login`
  - search the public model index

All endpoints live under api.hippius.com (HIPPIUS_API_URL env var to override
for local dev).
"""
import json
import os
from typing import Any, Optional

import httpx

from .constants import API_TOKEN_PATH, DEFAULT_API_URL, DEFAULT_CACHE_DIR, DEFAULT_HTTP_TIMEOUT


class ConsoleError(Exception):
    def __init__(self, status_code: int, body: Any):
        self.status_code = status_code
        self.body = body
        super().__init__(f"Hippius API error {status_code}: {body}")


def save_api_token(token: str) -> None:
    """Persist the user's console.hippius.com API token to disk."""
    os.makedirs(DEFAULT_CACHE_DIR, exist_ok=True)
    with open(API_TOKEN_PATH, "w") as f:
        f.write(token.strip())
    try:
        os.chmod(API_TOKEN_PATH, 0o600)
    except OSError:
        pass


def load_api_token() -> Optional[str]:
    """Return the cached console API token, or None if no token is saved."""
    if not os.path.exists(API_TOKEN_PATH):
        return None
    with open(API_TOKEN_PATH, "r") as f:
        tok = f.read().strip()
    return tok or None


def _headers(token: Optional[str] = None, *, require: bool = True) -> dict:
    """Build the Authorization + Accept headers for one console API call."""
    token = token or load_api_token()
    if not token and require:
        raise ConsoleError(401, (
            "Not logged in. Get your API token from console.hippius.com and "
            "run `hippius-hub login --hippius-token <token>`."
        ))
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Token {token}"
    return headers


def _request(method: str, path: str, *, token: Optional[str] = None,
             json_body: Any = None, params: Optional[dict] = None,
             require_auth: bool = True, base_url: Optional[str] = None) -> Any:
    """Send one HTTP request to the console API and decode the JSON response."""
    url = (base_url or DEFAULT_API_URL).rstrip("/") + path
    try:
        r = httpx.request(
            method,
            url,
            headers=_headers(token, require=require_auth),
            json=json_body,
            params=params,
            timeout=DEFAULT_HTTP_TIMEOUT,
        )
    except httpx.RequestError as e:
        raise ConsoleError(0, f"network error: {e}")

    if r.status_code >= 400:
        try:
            body = r.json()
        except (ValueError, json.JSONDecodeError):
            body = r.text
        raise ConsoleError(r.status_code, body)
    if r.status_code == 204 or not r.content:
        return None
    try:
        return r.json()
    except ValueError:
        return r.text


# -------- Registry --------

def list_plans():
    """Fetch the list of pricing plans from the console API."""
    return _request("GET", "/api/registry/plans/", require_auth=False)


def check_namespace(name: str):
    """Check whether a registry namespace is available."""
    return _request("GET", "/api/registry/namespaces/check/", params={"name": name})


def provision(namespace: str):
    """Create or revive the user's registry namespace. Returns creds (robot
    secret only on first creation)."""
    return _request("POST", "/api/registry/provision/", json_body={"namespace": namespace})


def provision_status():
    """Return the provisioning status of the user's projects."""
    return _request("GET", "/api/registry/provision/status/")


def me():
    """Return the active registry project (project, plan, quota, etc.)."""
    return _request("GET", "/api/registry/me/")


def me_robot():
    """Return the docker robot login for the active project (no secret)."""
    return _request("GET", "/api/registry/me/robot/")


def rotate_robot():
    """Rotate the docker robot secret and return the new credentials."""
    return _request("POST", "/api/registry/robot/rotate/")


def list_repositories(page: int = 1, page_size: int = 50):
    """List the active project's repositories with pagination."""
    return _request("GET", "/api/registry/repositories/",
                    params={"page": page, "page_size": page_size})


def _split_project_repo(repo: str) -> tuple:
    """Split a '<project>/<repo>' string into a (project, name) tuple."""
    if "/" not in repo:
        raise ValueError(f"repo must be '<project>/<repo>', got {repo!r}")
    project, name = repo.split("/", 1)
    return project, name


def list_artifacts(repo: str, page: int = 1, page_size: int = 50):
    """List artifacts inside one '<project>/<repo>' with pagination."""
    # The registry path `/api/registry/repositories/<project>/<repo>/artifacts/`
    # exists but only accepts DELETE. Listing artifacts is served by the model
    # index, which returns the same per-revision data plus the indexer's
    # parsed format/architecture/params/quantization.
    project, name = _split_project_repo(repo)
    res = _request("GET", f"/api/models/{project}/{name}/",
                   params={"page": page, "page_size": page_size})
    return (res or {}).get("artifacts", [])


def get_artifact(repo: str, reference: str):
    """Fetch one artifact by tag or sha256 digest from '<project>/<repo>'."""
    # Same routing story as list_artifacts: the registry GET returns 405, the
    # model-index endpoint serves the artifact detail.
    project, name = _split_project_repo(repo)
    return _request("GET", f"/api/models/{project}/{name}/{reference}/")


def delete_artifact(repo: str, reference: str):
    """Delete one artifact by tag or sha256 digest from '<project>/<repo>'."""
    project, name = _split_project_repo(repo)
    return _request("DELETE",
                    f"/api/registry/repositories/{project}/{name}/artifacts/{reference}/")


def usage():
    """Return live storage usage and 7-day history for the active project."""
    return _request("GET", "/api/registry/usage/")


def usage_per_repo(page: int = 1, page_size: int = 100):
    """Return per-repository storage usage with pagination."""
    return _request("GET", "/api/registry/usage/repositories/",
                    params={"page": page, "page_size": page_size})


def events():
    """Return the registry audit event stream for the active project."""
    return _request("GET", "/api/registry/events/")


def toggle_publicity(public: bool):
    """Flip the active project public or private (quota changes server-side)."""
    return _request("PATCH", "/api/registry/me/publicity/", json_body={"public": public})


# -------- Subscriptions (marketplace pallet) --------

def subscribe(plan_id: int, pay_upfront: Optional[int] = None):
    """Submit an on-chain subscription to a plan (optionally pay N months upfront)."""
    body: dict = {"plan_id": plan_id}
    if pay_upfront is not None:
        body["pay_upfront"] = pay_upfront
    return _request("POST", "/api/registry/subscribe/", json_body=body)


def list_subscriptions():
    """Return the user's active subscriptions (synced from chain)."""
    return _request("GET", "/api/registry/subscriptions/")


def cancel_subscription(subscription_id: int):
    """Cancel an on-chain subscription by its u32 SubscriptionId."""
    return _request("DELETE", f"/api/registry/subscriptions/{subscription_id}/")


# -------- Per-project API keys --------

def list_keys():
    """Return the per-project API keys (Harbor robots) for the active project."""
    return _request("GET", "/api/registry/keys/")


def create_key(name: str, role: str, expires_days: Optional[int] = None):
    """Create a role-scoped API key; the returned secret is shown only once."""
    body: dict = {"name": name, "role": role}
    if expires_days is not None:
        body["expires_days"] = expires_days
    return _request("POST", "/api/registry/keys/", json_body=body)


def show_key(key_id: int):
    """Show one API key by id (no secret)."""
    return _request("GET", f"/api/registry/keys/{key_id}/")


def rotate_key(key_id: int):
    """Rotate one API key's secret and return the new credentials."""
    return _request("POST", f"/api/registry/keys/{key_id}/rotate/")


def revoke_key(key_id: int):
    """Delete one API key irreversibly; its docker login stops working immediately."""
    return _request("DELETE", f"/api/registry/keys/{key_id}/")


# -------- Model index --------

def models_list(*, fmt: Optional[str] = None, architecture: Optional[str] = None,
                quantization: Optional[str] = None, min_params: Optional[int] = None,
                max_params: Optional[int] = None, q: Optional[str] = None,
                mine: bool = False, page: int = 1, page_size: int = 25):
    """Search the public AI model index with paginated, filtered results."""
    params: dict = {"page": page, "page_size": page_size}
    if fmt: params["format"] = fmt
    if architecture: params["architecture"] = architecture
    if quantization: params["quantization"] = quantization
    if min_params: params["min_params"] = min_params
    if max_params: params["max_params"] = max_params
    if q: params["q"] = q
    if mine: params["mine"] = "true"
    return _request("GET", "/api/models/", params=params, require_auth=False)


def models_formats():
    """Return the model index's filter facets (formats, architectures, quantizations)."""
    return _request("GET", "/api/models/formats/", require_auth=False)


def model_repo(project: str, repo: str):
    """Return all artifact versions for one '<project>/<repo>' from the model index."""
    return _request("GET", f"/api/models/{project}/{repo}/", require_auth=False)


def model_detail(project: str, repo: str, reference: str):
    """Return a single artifact's details (tag or sha256 digest) from the model index."""
    return _request("GET", f"/api/models/{project}/{repo}/{reference}/", require_auth=False)
