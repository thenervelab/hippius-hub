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
    if not os.path.exists(API_TOKEN_PATH):
        return None
    with open(API_TOKEN_PATH, "r") as f:
        tok = f.read().strip()
    return tok or None


def _headers(token: Optional[str] = None, *, require: bool = True) -> dict:
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
    return _request("GET", "/api/registry/plans/", require_auth=False)


def check_namespace(name: str):
    return _request("GET", "/api/registry/namespaces/check/", params={"name": name})


def provision(namespace: str):
    """Create or revive the user's registry namespace. Returns creds (robot
    secret only on first creation)."""
    return _request("POST", "/api/registry/provision/", json_body={"namespace": namespace})


def provision_status():
    return _request("GET", "/api/registry/provision/status/")


def me():
    return _request("GET", "/api/registry/me/")


def me_robot():
    return _request("GET", "/api/registry/me/robot/")


def rotate_robot():
    return _request("POST", "/api/registry/robot/rotate/")


def list_repositories(page: int = 1, page_size: int = 50):
    return _request("GET", "/api/registry/repositories/",
                    params={"page": page, "page_size": page_size})


def _split_project_repo(repo: str) -> tuple:
    if "/" not in repo:
        raise ValueError(f"repo must be '<project>/<repo>', got {repo!r}")
    project, name = repo.split("/", 1)
    return project, name


def list_artifacts(repo: str, page: int = 1, page_size: int = 50):
    # The registry path `/api/registry/repositories/<project>/<repo>/artifacts/`
    # exists but only accepts DELETE. Listing artifacts is served by the model
    # index, which returns the same per-revision data plus the indexer's
    # parsed format/architecture/params/quantization.
    project, name = _split_project_repo(repo)
    res = _request("GET", f"/api/models/{project}/{name}/",
                   params={"page": page, "page_size": page_size})
    return (res or {}).get("artifacts", [])


def get_artifact(repo: str, reference: str):
    # Same routing story as list_artifacts: the registry GET returns 405, the
    # model-index endpoint serves the artifact detail.
    project, name = _split_project_repo(repo)
    return _request("GET", f"/api/models/{project}/{name}/{reference}/")


def delete_artifact(repo: str, reference: str):
    project, name = _split_project_repo(repo)
    return _request("DELETE",
                    f"/api/registry/repositories/{project}/{name}/artifacts/{reference}/")


def usage():
    return _request("GET", "/api/registry/usage/")


def usage_per_repo(page: int = 1, page_size: int = 100):
    return _request("GET", "/api/registry/usage/repositories/",
                    params={"page": page, "page_size": page_size})


def events():
    return _request("GET", "/api/registry/events/")


def toggle_publicity(public: bool):
    return _request("PATCH", "/api/registry/me/publicity/", json_body={"public": public})


# -------- Subscriptions (marketplace pallet) --------

def subscribe(plan_id: int, pay_upfront: Optional[int] = None):
    body: dict = {"plan_id": plan_id}
    if pay_upfront is not None:
        body["pay_upfront"] = pay_upfront
    return _request("POST", "/api/registry/subscribe/", json_body=body)


def list_subscriptions():
    return _request("GET", "/api/registry/subscriptions/")


def cancel_subscription(subscription_id: int):
    return _request("DELETE", f"/api/registry/subscriptions/{subscription_id}/")


# -------- Model index --------

def models_list(*, fmt: Optional[str] = None, architecture: Optional[str] = None,
                quantization: Optional[str] = None, min_params: Optional[int] = None,
                max_params: Optional[int] = None, q: Optional[str] = None,
                mine: bool = False, page: int = 1, page_size: int = 25):
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
    return _request("GET", "/api/models/formats/", require_auth=False)


def model_repo(project: str, repo: str):
    return _request("GET", f"/api/models/{project}/{repo}/", require_auth=False)


def model_detail(project: str, repo: str, reference: str):
    return _request("GET", f"/api/models/{project}/{repo}/{reference}/", require_auth=False)
