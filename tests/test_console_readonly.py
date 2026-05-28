"""Schema tests for every read-only `console.*` function.

Until now, nothing exercised `hippius_hub.console` against the live
api.hippius.com — every CLI `registry`/`models` subcommand and the
`HIPPIUS_API_URL` integration were one backend schema-change away from
breaking with no test signal. Each test asserts the keys the CLI actually
dereferences (`p['name']`, `r['storage_used_bytes']`, etc.) so a server
contract drift fails here, not in user CLI runs.

Pre-req: `HIPPIUS_TEST_CONSOLE_TOKEN` env var with a token from
https://console.hippius.com/dashboard/settings — tests skip otherwise.
"""
import uuid

import pytest

from hippius_hub import console
from hippius_hub.console import ConsoleError


pytestmark = pytest.mark.e2e


def _assert_keys(d: dict, keys, *, where: str = ""):
    """All keys must be present (values may be None for nullable fields)."""
    missing = [k for k in keys if k not in d]
    assert not missing, f"{where} missing keys: {missing} (got {list(d)})"


# ---------- public, unauthenticated reads ----------

def test_list_plans_returns_pricing_rows():
    """`registry plans` reads each row's name/price/storage/max_projects/features."""
    plans = console.list_plans()
    assert isinstance(plans, list) and plans, "expected at least one plan"
    p = plans[0]
    _assert_keys(p, ["name", "price_credits", "private_storage_gb",
                     "public_storage_gb", "max_projects"], where="plan")
    # `features` is optional per CLI rendering (`p.get("features", [])`).


def test_models_formats_returns_filter_lists():
    """`models formats` exposes formats / architectures / quantizations arrays
    so users discover valid filter values."""
    res = console.models_formats()
    _assert_keys(res, ["formats", "architectures", "quantizations"], where="formats")
    assert isinstance(res["formats"], list)
    assert isinstance(res["architectures"], list)
    assert isinstance(res["quantizations"], list)


def test_models_list_no_filters_returns_paginated_results():
    """The flat `models list` view: paginated `results` array + total."""
    res = console.models_list(page=1, page_size=5)
    _assert_keys(res, ["results", "total"], where="models_list")
    assert isinstance(res["results"], list)
    assert len(res["results"]) <= 5
    if res["results"]:
        m = res["results"][0]
        _assert_keys(m, ["project", "repo", "format"], where="models_list[0]")


# ---------- authenticated console endpoints ----------

def test_check_namespace_taken(console_logged_in, console_test_project):
    """An existing project's name must come back unavailable."""
    res = console.check_namespace(console_test_project)
    assert isinstance(res, dict)
    assert "available" in res
    assert res["available"] is False


def test_check_namespace_random_uuid_is_available(console_logged_in):
    """A fresh random name must be available — sanity-checks the negative path."""
    name = f"avail-{uuid.uuid4().hex[:12]}"
    res = console.check_namespace(name)
    assert res.get("available") is True


def test_me_returns_active_project(console_logged_in):
    """`registry me` reads project_name, plan_name, status, public, quota,
    registry_url, robot_login."""
    me = console.me()
    _assert_keys(me, ["project_name", "status", "public", "registry_url"], where="me")
    # plan_name / robot_login / storage_quota_bytes are nullable in early states.


def test_provision_status_shape(console_logged_in):
    """`registry status` reads `projects[*].project_name / status / plan_name`."""
    res = console.provision_status()
    _assert_keys(res, ["projects"], where="provision_status")
    assert isinstance(res["projects"], list)
    for p in res["projects"]:
        _assert_keys(p, ["project_name", "status"], where="provision_status.projects[]")


def test_list_repositories_shape(console_logged_in):
    """`registry repos` reads name / artifact_count / pull_count / update_time."""
    rows = console.list_repositories(page=1, page_size=5)
    assert isinstance(rows, list)
    if rows:
        r = rows[0]
        _assert_keys(r, ["name"], where="repositories[]")
        # artifact_count / pull_count / update_time are read with `.get(...)`
        # in the CLI so they're effectively optional — don't enforce here.


def test_list_artifacts_known_repo(console_logged_in, console_test_project, test_repo):
    """`registry artifacts` reads digest / all_tags / primary_tag / total_size_bytes
    / indexed_at. Resolves the repo as `<active-project>/<repo-tail>` so the test
    works regardless of which CI account is in use."""
    repo_tail = test_repo.split("/", 1)[1] if "/" in test_repo else test_repo
    # Use the active project — the seeded `test/e2e-client` repo is created
    # under whatever project the CI account owns.
    try:
        rows = console.list_artifacts(f"{console_test_project}/{repo_tail}", page=1, page_size=5)
    except ConsoleError as e:
        if e.status_code == 404:
            pytest.skip(f"Test account project lacks repo {repo_tail!r} — seed it first")
        raise
    assert isinstance(rows, list)
    if rows:
        a = rows[0]
        _assert_keys(a, ["digest"], where="artifacts[]")


def test_usage_returns_live_and_history(console_logged_in):
    """`registry usage` reads `live.storage_used_bytes` and `history[]` rows."""
    u = console.usage()
    _assert_keys(u, ["live"], where="usage")
    assert isinstance(u.get("history") or [], list)
    live = u["live"]
    if live:  # may be empty pre-provision
        _assert_keys(live, ["storage_used_bytes", "storage_quota_bytes"], where="usage.live")


def test_usage_per_repo_paginated(console_logged_in):
    """Documented endpoint that's unused by the CLI today — covering it now
    prevents silent rot."""
    res = console.usage_per_repo(page=1, page_size=5)
    assert isinstance(res, (list, dict))


def test_events_endpoint_responds(console_logged_in):
    """Per-project audit events stream — schema is server-owned, just confirm
    it's reachable and returns a JSON shape."""
    res = console.events()
    assert isinstance(res, (list, dict))


def test_list_subscriptions_returns_list(console_logged_in):
    """`registry subscriptions` reads subscription_id / plan_name / paid_per_month
    / next_charge_unix_day / synced_at / active / cancelled_at."""
    rows = console.list_subscriptions()
    rows = rows or []
    assert isinstance(rows, list)
    if rows:
        r = rows[0]
        _assert_keys(r, ["subscription_id", "plan_name"], where="subscriptions[]")


def test_list_keys_returns_list(console_logged_in):
    """`registry keys list` reads id / name / role / login / expires_at / last_used_at."""
    rows = console.list_keys()
    rows = rows or []
    assert isinstance(rows, list)
    if rows:
        k = rows[0]
        _assert_keys(k, ["id", "name", "role", "login"], where="keys[]")


def test_model_repo_known_repo(console_logged_in, console_test_project, test_repo):
    """`models show <project>/<repo>` reads `artifacts[].primary_tag / parameter_count
    / total_size_bytes / indexed_at` plus a top-level `total`."""
    repo_tail = test_repo.split("/", 1)[1] if "/" in test_repo else test_repo
    try:
        res = console.model_repo(console_test_project, repo_tail)
    except ConsoleError as e:
        if e.status_code == 404:
            pytest.skip(f"No model rows yet for {repo_tail!r}")
        raise
    _assert_keys(res, ["artifacts"], where="model_repo")
    assert isinstance(res["artifacts"], list)


def test_model_detail_known_revision(console_logged_in, console_test_project, test_repo):
    """`models show <project>/<repo> <ref>` reads format / architecture /
    parameter_count / quantization / total_size_bytes / digest / files / pull_command."""
    repo_tail = test_repo.split("/", 1)[1] if "/" in test_repo else test_repo
    try:
        repo_res = console.model_repo(console_test_project, repo_tail)
    except ConsoleError as e:
        if e.status_code == 404:
            pytest.skip(f"No model rows yet for {repo_tail!r}")
        raise
    artifacts = repo_res.get("artifacts") or []
    if not artifacts:
        pytest.skip(f"No artifacts indexed under {console_test_project}/{repo_tail}")
    ref = artifacts[0].get("primary_tag") or artifacts[0].get("digest")
    if not ref:
        pytest.skip("Artifact row has neither primary_tag nor digest")

    detail = console.model_detail(console_test_project, repo_tail, ref)
    _assert_keys(detail, ["project", "repo", "files"], where="model_detail")
    assert isinstance(detail["files"], list)
    for f in detail["files"]:
        _assert_keys(f, ["filename", "format", "size_bytes"], where="model_detail.files[]")
